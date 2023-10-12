use anyhow::anyhow;
use base64::{engine::general_purpose::STANDARD, Engine};
use pub_sub_client::{
    Error, PubSubClient, PulledMessage, RawPublishedMessage, RawPulledMessageEnvelope,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, env, error::Error as _, time::Duration};

const TOPIC_ID: &str = "test";
const SUBSCRIPTION_ID: &str = "test";

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
enum Message {
    Foo { text: String },
    Bar { text: String },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    if let Err(e) = run().await {
        eprintln!("ERROR: {e}");
        if let Some(e) = e.source() {
            eprintln!("SOURCE: {e}");
        }
    }
}

async fn run() -> Result<(), Error> {
    let dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let key_path = format!("{dir}/secrets/active-road-365118-2eca6b7b8fd9.json");
    let pub_sub_client = PubSubClient::new(key_path, Duration::from_secs(30))?;

    let messages = vec!["Hello", "from pub-sub-client"]
        .iter()
        .map(|s| STANDARD.encode(json!({ "text": s }).to_string()))
        .map(|data| {
            RawPublishedMessage::new(data)
                .with_attributes(HashMap::from([("type".to_string(), "Foo".to_string())]))
        })
        .collect::<Vec<_>>();
    let message_ids = pub_sub_client.publish_raw(TOPIC_ID, messages, None).await?;
    let message_ids = message_ids.join(", ");
    println!("Published messages with IDs: {message_ids}");

    let pulled_messages = pub_sub_client
        .pull_with_transform::<Message, _>(
            SUBSCRIPTION_ID,
            42,
            Some(Duration::from_secs(45)),
            transform,
        )
        .await?;

    for pulled_message in pulled_messages {
        let PulledMessage {
            ack_id,
            message,
            attributes: _,
            id,
            publish_time: _,
            ordering_key: _,
            delivery_attempt,
        } = pulled_message;
        println!(
            "Pulled message `{message:?}` with ID {id} and {delivery_attempt}. delivery attempt"
        );

        pub_sub_client
            .acknowledge(
                SUBSCRIPTION_ID,
                vec![&ack_id],
                Some(Duration::from_secs(10)),
            )
            .await?;
        println!("Successfully acknowledged message with ID {id}");
    }

    Ok(())
}

fn transform(
    received_message: &RawPulledMessageEnvelope,
    value: Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let attributes = &received_message.message.attributes;
    match attributes {
        Some(attributes) => match attributes.get("type") {
            Some(t) => match value {
                Value::Object(mut map) => {
                    map.insert("type".to_string(), Value::String(t.to_string()));
                    Ok(Value::Object(map))
                }
                other => Err(anyhow!("Unexpected JSON value `{other}`").into()),
            },
            None => {
                let e = anyhow!(
                    "Missing `type` attribute, message ID is `{}`",
                    received_message.message.id
                );
                Err(e.into())
            }
        },
        None => {
            let e = anyhow!(
                "Missing attributes, message ID is `{}`",
                received_message.message.id
            );
            Err(e.into())
        }
    }
}
