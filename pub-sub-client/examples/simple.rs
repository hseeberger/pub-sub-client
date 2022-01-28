use pub_sub_client::error::Error;
use pub_sub_client::publisher::PublisherMessage;
use pub_sub_client::PubSubClient;
use serde::{Deserialize, Serialize};
use std::error::Error as _;
use std::time::Duration;

const TOPIC_ID: &str = "test";
const SUBSCRIPTION_ID: &str = "test";

#[derive(Debug, Deserialize, Serialize)]
struct Message {
    text: String,
}

impl PublisherMessage for Message {}

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
    let pub_sub_client = PubSubClient::new(
        "secrets/cryptic-hawk-336616-e228f9680cbc.json",
        Duration::from_secs(30),
    )?;

    let messages = vec!["Hello", "from pub-sub-client"]
        .iter()
        .map(|s| s.to_string())
        .map(|text| Message { text })
        .collect::<Vec<_>>();
    let message_ids = pub_sub_client
        .publish(TOPIC_ID, messages, None, None)
        .await?;
    let message_ids = message_ids.join(", ");
    println!("Published messages with IDs: {message_ids}");

    let pulled_messages = pub_sub_client
        .pull::<Message>(SUBSCRIPTION_ID, 42, None)
        .await?;
    for pulled_message in pulled_messages {
        let pulled_message = pulled_message?;
        let text = pulled_message.message.text;
        println!("Pulled message with text \"{text}\"");

        pub_sub_client
            .acknowledge(SUBSCRIPTION_ID, vec![&pulled_message.ack_id], None)
            .await?;
        let id = pulled_message.id;
        println!("Successfully acknowledged message with ID {id}");
    }

    Ok(())
}
