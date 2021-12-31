use pub_sub_client::PubSubClient;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use std::{env, vec};
use testcontainers::clients::Cli;
use testcontainers::images::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};

const PROJECT_ID: &str = "cryptic-hawk-336616";
const TOPIC_ID: &str = "test-topic";
const SUBSCRIPTION_ID: &str = "test-subscription";
const TEXT: &str = "test-text";

#[derive(Debug, Deserialize, PartialEq, Eq)]
enum Message {
    Foo { text: String },
    Bar { text: String },
}

#[tokio::test]
async fn test() {
    // Set up testcontainers
    let docker_cli = Cli::default();
    let node = docker_cli.run(CloudSdk::pubsub());
    let base_url = format!("http://localhost:{}", node.get_host_port(PUBSUB_PORT));
    let topic_name = format!("projects/{}/topics/{}", PROJECT_ID, TOPIC_ID);
    let subscription_name = format!("projects/{}/subscriptions/{}", PROJECT_ID, SUBSCRIPTION_ID);

    // We interact with Pub/Sub via HTTP
    let reqwest_client = Client::new();

    // Create topic
    let response = reqwest_client
        .put(format!("{}/v1/{}", base_url, topic_name))
        .send()
        .await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Create subscription
    let response = reqwest_client
        .put(format!("{}/v1/{}", base_url, subscription_name))
        .json(&json!({ "topic": topic_name }))
        .send()
        .await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Publish to topic
    let foo = base64::encode(json!({ "Foo": { "text": TEXT } }).to_string());
    let bar = base64::encode(json!({ "Bar": { "text": TEXT } }).to_string());
    let response = reqwest_client
        .post(format!("{}/v1/{}:publish", base_url, topic_name))
        .json(&json!(
          {
            "messages": [
              { "data": foo },
              { "data": bar },
              { "data": bar }
            ]
          }
        ))
        .send()
        .await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Create PubSubClient
    env::set_var("PUB_SUB_BASE_URL", base_url);
    let pub_sub_client = PubSubClient::new(
        "secrets/cryptic-hawk-336616-e228f9680cbc.json",
        Duration::from_secs(30),
    );
    env::set_var("PUB_SUB_BASE_URL", "");
    assert!(pub_sub_client.is_ok());
    let pub_sub_client = pub_sub_client.unwrap();

    // Pull
    let response = pub_sub_client.pull::<Message>(SUBSCRIPTION_ID, 42).await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.len(), 3);

    assert!(response[0].is_ok());
    let ack_id_1 = &response[0].as_ref().unwrap().ack_id[..];
    assert_eq!(
        response[0].as_ref().unwrap().message,
        Message::Foo {
            text: TEXT.to_string()
        }
    );

    assert!(response[1].is_ok());
    let ack_id_2 = &response[1].as_ref().unwrap().ack_id[..];
    assert_eq!(
        response[1].as_ref().unwrap().message,
        Message::Bar {
            text: TEXT.to_string()
        }
    );

    assert!(response[2].is_ok());
    let message_id_3 = &response[2].as_ref().unwrap().id[..];
    assert_eq!(
        response[2].as_ref().unwrap().message,
        Message::Bar {
            text: TEXT.to_string()
        }
    );

    // Acknowledge
    let ack_ids = vec![ack_id_1, ack_id_2];
    let response = pub_sub_client.acknowledge(SUBSCRIPTION_ID, ack_ids).await;
    assert!(response.is_ok());

    // Pull again
    let response = pub_sub_client.pull::<Message>(SUBSCRIPTION_ID, 42).await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.len(), 1);

    assert!(response[0].is_ok());
    assert_eq!(response[0].as_ref().unwrap().id, message_id_3);

    // Acknowledge with invalid ACK ID
    let response = pub_sub_client
        .acknowledge(SUBSCRIPTION_ID, vec!["invalid"])
        .await;
    assert!(response.is_err());
}
