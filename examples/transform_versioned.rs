use anyhow::anyhow;
use pub_sub_client::{Error, MessageEnvelope, PubSubClient, ReceivedMessage};
use serde::Deserialize;
use serde_json::{json, Value};
use std::error::Error as _;
use std::time::Duration;

const SUBSCRIPTION: &str = "test";

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
enum Message {
    Foo { text: String },
    Bar { text: String },
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ERROR: {e}");
        if let Some(e) = e.source() {
            eprintln!("SOURCE: {e}");
        }
    }
}

async fn run() -> Result<(), Error> {
    let pub_sub_client = PubSubClient::new(
        "secrets/cryptic-hawk-336616-e228f9680cbc.json",
        Duration::from_secs(30),
    )?;

    let msg_envelopes = pub_sub_client
        .pull_with_transform::<Message, _>(
            SUBSCRIPTION,
            42,
            Some(Duration::from_secs(45)),
            transform,
        )
        .await?;

    for msg_envelope in msg_envelopes {
        let MessageEnvelope {
            id,
            ack_id,
            attributes: _,
            message,
            delivery_attempt,
        } = msg_envelope?;
        println!("id: {id}, message: {message:?}, delivery_attempt: {delivery_attempt}");

        pub_sub_client
            .acknowledge(SUBSCRIPTION, vec![&ack_id], Some(Duration::from_secs(10)))
            .await?;
        println!("Successfully acknowledged");
    }

    Ok(())
}

fn transform(
    received_message: &ReceivedMessage,
    mut value: Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let attributes = &received_message.message.attributes;
    match attributes.get("version").map(|v| &v[..]).unwrap_or("v1") {
        "v1" => {
            let mut type_keys = attributes
                .keys()
                .filter(|key| **key == "type" || key.starts_with("type."))
                .map(|key| (&key[..], key.split(".").skip(1).collect::<Vec<_>>()))
                .collect::<Vec<_>>();
            type_keys.sort_unstable_by(|v1, v2| v2.1.len().cmp(&v1.1.len()));
            for (type_key, json_path) in type_keys {
                let sub_value = json_path
                    .iter()
                    .fold(Some(&mut value), |v, k| v.and_then(|v| v.get_mut(k)));
                if let Some(sub_value) = sub_value {
                    let tpe = attributes.get(type_key).unwrap().to_string();
                    *sub_value = json!({ tpe: sub_value });
                }
            }
            Ok(value)
        }
        "v2" => Ok(value),
        unknown => Err(anyhow!("Unknow version `{unknown}`").into()),
    }
}
