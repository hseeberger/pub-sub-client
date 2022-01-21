use pub_sub_client::{Error, PubSubClient};
use serde::Deserialize;
use std::error::Error as _;
use std::time::Duration;

const SUBSCRIPTION: &str = "test";

#[derive(Debug, Deserialize)]
struct Message {
    text: String,
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

    let envelopes = pub_sub_client
        .pull::<Message>(SUBSCRIPTION, 42, None)
        .await?;

    for envelope in envelopes {
        let envelope = envelope?;

        let text = envelope.message.text;
        println!("Message text: {text}");

        pub_sub_client
            .acknowledge(SUBSCRIPTION, vec![&envelope.ack_id], None)
            .await?;
        println!("Successfully acknowledged");
    }

    Ok(())
}
