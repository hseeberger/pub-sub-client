use pub_sub_client::{Error, PubSubClient};
use serde::Deserialize;
use std::error::Error as _;
use std::time::Duration;

const SUBSCRIPTION: &str = "projects/cryptic-hawk-336616/subscriptions/test";

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
enum Message {
    Foo { text: String },
    Bar { text: String },
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ERROR: {}", e);
        if let Some(e) = e.source() {
            eprintln!("SOURCE: {}", e);
        }
    }
}

async fn run() -> Result<(), Error> {
    let pub_sub_client = PubSubClient::new(
        "secrets/cryptic-hawk-336616-e228f9680cbc.json",
        Duration::from_secs(30),
    )?;

    let msg_envelopes = pub_sub_client
        .pull_insert_attribute::<Message>(SUBSCRIPTION, 42, "type")
        .await?;

    for msg_envelope in msg_envelopes {
        match msg_envelope {
            Ok(m) => {
                println!(
                    "id: {}, message: {:?}, delivery_attempt: {}",
                    m.id, m.message, m.delivery_attempt
                );
                match pub_sub_client
                    .acknowledge(SUBSCRIPTION, vec![&m.ack_id])
                    .await
                {
                    Ok(_) => println!("Successfully acknowledged"),
                    Err(e) => eprintln!("ERROR: {}", e),
                }
            }
            Err(e) => eprintln!("ERROR: {}", e),
        }
    }

    Ok(())
}
