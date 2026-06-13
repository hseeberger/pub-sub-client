use pub_sub_client::{Error, PubSubClient};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{env, error::Error as _, time::Duration};

const TOPIC_ID: &str = "test";
const SUBSCRIPTION_ID: &str = "test";

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    text: String,
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
        .map(|s| s.to_string())
        .map(|text| Message { text })
        .collect::<Vec<_>>();
    let message_ids = pub_sub_client
        .publish(TOPIC_ID, messages, None, None)
        .await?;
    let message_ids = message_ids.join(", ");
    println!("published messages with IDs: {message_ids}");

    let pulled_messages = pub_sub_client
        .pull::<Message>(SUBSCRIPTION_ID, 42, None)
        .await?;

    for pulled_message in pulled_messages {
        match pulled_message.message {
            Ok(m) => println!("pulled message with text \"{}\"", m.text),
            Err(e) => eprintln!("ERROR: {e}"),
        }

        pub_sub_client
            .acknowledge(SUBSCRIPTION_ID, vec![&pulled_message.ack_id], None)
            .await?;
        println!("acknowledged message with ID {}", pulled_message.id);
    }

    Ok(())
}

// Treat an "already exists" response (409 Conflict) as success, but surface any other error.
fn ignore_already_exists(result: Result<(), Error>) -> Result<(), Error> {
    match result {
        Err(Error::UnexpectedHttpStatusCode(status, _)) if status == StatusCode::CONFLICT => Ok(()),
        result => result,
    }
}
