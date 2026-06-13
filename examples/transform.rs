use anyhow::anyhow;
use base64::{Engine, engine::general_purpose::STANDARD};
use pub_sub_client::{
    Error, PubSubClient, PulledMessage, RawPublishedMessage, RawPulledMessageEnvelope,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
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
    let key_path = env::var("SERVICE_ACCOUNT_PATH").expect("SERVICE_ACCOUNT_PATH must be set");
    let pub_sub_client = PubSubClient::new(key_path, Duration::from_secs(30))?;

    // Create the topic and subscription, ignoring the error if they already exist.
    ignore_already_exists(pub_sub_client.create_topic(TOPIC_ID, None).await)?;
    ignore_already_exists(
        pub_sub_client
            .create_subscription(SUBSCRIPTION_ID, TOPIC_ID, None)
            .await,
    )?;

    let messages = ["Hello", "from pub-sub-client"]
        .iter()
        .map(|s| STANDARD.encode(json!({ "text": s }).to_string()))
        .map(|data| {
            RawPublishedMessage::new(data)
                .with_attributes(HashMap::from([("type".to_string(), "Foo".to_string())]))
        })
        .collect::<Vec<_>>();
    let message_ids = pub_sub_client.publish_raw(TOPIC_ID, messages, None).await?;
    let message_ids = message_ids.join(", ");
    println!("published messages with IDs: {message_ids}");

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
            "pulled message `{message:?}` with ID {id} and {delivery_attempt}. delivery attempt"
        );

        pub_sub_client
            .acknowledge(
                SUBSCRIPTION_ID,
                vec![&ack_id],
                Some(Duration::from_secs(10)),
            )
            .await?;
        println!("acknowledged message with ID {id}");
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
                other => Err(anyhow!("unexpected JSON value `{other}`").into()),
            },
            None => {
                let e = anyhow!(
                    "missing `type` attribute, message ID is `{}`",
                    received_message.message.id
                );
                Err(e.into())
            }
        },
        None => {
            let e = anyhow!(
                "missing attributes, message ID is `{}`",
                received_message.message.id
            );
            Err(e.into())
        }
    }
}

// Treat an "already exists" response (409 Conflict) as success, but surface any other error.
fn ignore_already_exists(result: Result<(), Error>) -> Result<(), Error> {
    match result {
        Err(Error::UnexpectedHttpStatusCode(status, _)) if status == StatusCode::CONFLICT => Ok(()),
        result => result,
    }
}
