use pub_sub_client::publisher::PubSubMessage;
use pub_sub_client::publisher::PublisherMessage;
use pub_sub_client::PubSubClient;
use pub_sub_client_derive::PublisherMessage;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use std::{env, vec};
use testcontainers::clients::Cli;
use testcontainers::images::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};

const PROJECT_ID: &str = "cryptic-hawk-336616";
const TOPIC_ID: &str = "test-topic";
const SUBSCRIPTION_ID: &str = "test-subscription";
const TEXT: &str = "test-text";

#[derive(Debug, Deserialize, Serialize, PublisherMessage, PartialEq)]
enum Message {
    Foo { text: String },
    Bar { text: String },
}

#[tokio::test]
async fn test() {
    // Set up testcontainers
    let docker_cli = Cli::default();
    let node = docker_cli.run(CloudSdk::pubsub());
    let pubsub_port = node.get_host_port(PUBSUB_PORT);
    let base_url = format!("http://localhost:{pubsub_port}");
    let topic_name = format!("projects/{PROJECT_ID}/topics/{TOPIC_ID}");
    let subscription_name = format!("projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}");

    // For management we have to interact with Pub/Sub via HTTP
    let reqwest_client = Client::new();

    // Create topic
    let response = reqwest_client
        .put(format!("{base_url}/v1/{topic_name}"))
        .send()
        .await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Create subscription
    let response = reqwest_client
        .put(format!("{base_url}/v1/{subscription_name}"))
        .json(&json!({ "topic": topic_name }))
        .send()
        .await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Create PubSubClient
    // Notice: GitHub Actions write the `GCP_SERVICE_ACCOUNT` secret to the below key path,
    // locally `secrets/cryptic-hawk-336616-e228f9680cbc.json.gpg` must be decrypted.
    env::set_var("PUB_SUB_BASE_URL", base_url);
    let pub_sub_client = PubSubClient::new(
        "secrets/cryptic-hawk-336616-e228f9680cbc.json",
        Duration::from_secs(30),
    );
    env::set_var("PUB_SUB_BASE_URL", "");
    assert!(pub_sub_client.is_ok());
    let pub_sub_client = pub_sub_client.unwrap();

    // Publish raw
    let foo = base64::encode(json!({ "Foo": { "text": TEXT } }).to_string());
    let messages = vec![PubSubMessage::new(foo)];
    let result = pub_sub_client
        .publish_raw(TOPIC_ID, messages, Some(Duration::from_secs(10)))
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 1);

    // Publish agian, typed
    let messages = vec![
        Message::Bar {
            text: TEXT.to_string(),
        },
        Message::Bar {
            text: TEXT.to_string(),
        },
    ];
    let result = pub_sub_client
        .publish(TOPIC_ID, messages, None, Some(Duration::from_secs(10)))
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 2);

    // Pull typed
    let result = pub_sub_client
        .pull::<Message>(SUBSCRIPTION_ID, 42, Some(Duration::from_secs(45)))
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 3);

    assert!(result[0].is_ok());
    let ack_id_1 = &result[0].as_ref().unwrap().ack_id[..];
    assert_eq!(
        result[0].as_ref().unwrap().message,
        Message::Foo {
            text: TEXT.to_string()
        }
    );

    assert!(result[1].is_ok());
    let ack_id_2 = &result[1].as_ref().unwrap().ack_id[..];
    assert_eq!(
        result[1].as_ref().unwrap().message,
        Message::Bar {
            text: TEXT.to_string()
        }
    );

    assert!(result[2].is_ok());
    let ack_id_3 = &result[2].as_ref().unwrap().ack_id[..];
    let message_id_3 = &result[2].as_ref().unwrap().id[..];
    assert_eq!(
        result[2].as_ref().unwrap().message,
        Message::Bar {
            text: TEXT.to_string()
        }
    );

    // Acknowledge
    let ack_ids = vec![ack_id_1, ack_id_2];
    let result = pub_sub_client
        .acknowledge(SUBSCRIPTION_ID, ack_ids, Some(Duration::from_secs(10)))
        .await;
    assert!(result.is_ok());

    // Pull again, raw
    let result = pub_sub_client
        .pull_raw(SUBSCRIPTION_ID, 42, Some(Duration::from_secs(45)))
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].pub_sub_message.id, message_id_3);

    // Acknowledge with invalid ACK ID
    let response = pub_sub_client
        .acknowledge(
            SUBSCRIPTION_ID,
            vec!["invalid"],
            Some(Duration::from_secs(10)),
        )
        .await;
    assert!(response.is_err());

    // Acknowledge missing message
    let ack_ids = vec![ack_id_3];
    let result = pub_sub_client
        .acknowledge(SUBSCRIPTION_ID, ack_ids, Some(Duration::from_secs(10)))
        .await;
    assert!(result.is_ok());

    // Publish raw, only attributes
    let attributes = HashMap::from([("foo".to_string(), "bar".to_string())]);
    let messages = vec![PubSubMessage::default().with_attributes(attributes)];
    let result = pub_sub_client
        .publish_raw(TOPIC_ID, messages, Some(Duration::from_secs(10)))
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 1);

    // Pull again, raw
    let result = pub_sub_client
        .pull_raw(SUBSCRIPTION_ID, 42, Some(Duration::from_secs(45)))
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].pub_sub_message.data.is_none());
    assert_eq!(
        result[0].pub_sub_message.attributes,
        HashMap::from([("foo".to_string(), "bar".to_string(),)])
    );

    // Acknowledge
    let ack_ids = vec![&result[0].ack_id[..]];
    let result = pub_sub_client
        .acknowledge(SUBSCRIPTION_ID, ack_ids, Some(Duration::from_secs(10)))
        .await;
    assert!(result.is_ok());

    // Publish typed with attributes
    let messages = vec![(
        Message::Foo {
            text: TEXT.to_string(),
        },
        HashMap::from([("version".to_string(), "v1".to_string())]),
    )];
    let result = pub_sub_client
        .publish(TOPIC_ID, messages, None, Some(Duration::from_secs(10)))
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 1);

    // Pull typed
    let result = pub_sub_client
        .pull::<Message>(SUBSCRIPTION_ID, 42, Some(Duration::from_secs(45)))
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 1);

    eprintln!("{:?}", result[0]);
    assert!(result[0].is_ok());
    assert_eq!(
        result[0].as_ref().unwrap().message,
        Message::Foo {
            text: TEXT.to_string()
        }
    );
    assert_eq!(
        result[0].as_ref().unwrap().attributes,
        HashMap::from([("version".to_string(), "v1".to_string())])
    );
}
