use base64::{engine::general_purpose::STANDARD, Engine};
use pub_sub_client::{PubSubClient, PublishedMessage, RawPublishedMessage};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::HashMap, env, time::Duration, vec};
use testcontainers::clients::Cli;
use testcontainers_modules::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};

const PROJECT_ID: &str = "active-road-365118";
const TOPIC_ID: &str = "test";
const SUBSCRIPTION_ID: &str = "test";
const TEXT: &str = "test-text";

#[derive(Debug, Deserialize, Serialize, PublishedMessage, PartialEq)]
enum Message {
    Foo { text: String },
    Bar { text: String },
}

#[tokio::test]
async fn test() {
    // Set up testcontainers
    let docker_cli = Cli::default();
    let node = docker_cli.run(CloudSdk::pubsub());
    let pubsub_port = node.get_host_port_ipv4(PUBSUB_PORT);
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
    // locally the file must be decrypted.
    env::set_var("PUB_SUB_BASE_URL", base_url);
    let pub_sub_client = PubSubClient::new(
        "secrets/active-road-365118-2eca6b7b8fd9.json",
        Duration::from_secs(30),
    );
    env::set_var("PUB_SUB_BASE_URL", "");
    assert!(pub_sub_client.is_ok());
    let pub_sub_client = pub_sub_client.unwrap();

    // Publish raw
    let foo = STANDARD.encode(json!({ "Foo": { "text": TEXT } }).to_string());
    let messages = vec![RawPublishedMessage::new(foo)];
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

    let ack_id_1 = &result[0].ack_id[..];
    assert!(result[0].message.is_ok());
    let message = result[0].message.as_ref().unwrap();
    assert_eq!(
        *message,
        Message::Foo {
            text: TEXT.to_string()
        }
    );

    let ack_id_2 = &result[1].ack_id[..];
    assert!(result[1].message.is_ok());
    let message = result[1].message.as_ref().unwrap();
    assert_eq!(
        *message,
        Message::Bar {
            text: TEXT.to_string()
        }
    );

    let ack_id_3 = &result[2].ack_id[..];
    let message_id_3 = &result[2].id[..];
    assert!(result[2].message.is_ok());
    let message = result[2].message.as_ref().unwrap();
    assert_eq!(
        *message,
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
    assert_eq!(result[0].message.id, message_id_3);

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
    let messages = vec![RawPublishedMessage::default().with_attributes(attributes)];
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
    assert!(result[0].message.data.is_none());
    assert_eq!(
        result[0].message.attributes,
        Some(HashMap::from([("foo".to_string(), "bar".to_string(),)]))
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

    assert!(result[0].message.is_ok());
    let message = result[0].message.as_ref().unwrap();
    assert_eq!(
        *message,
        Message::Foo {
            text: TEXT.to_string()
        }
    );
    assert_eq!(
        result[0].attributes,
        Some(HashMap::from([("version".to_string(), "v1".to_string())]))
    );
}
