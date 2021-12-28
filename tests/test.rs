use std::env;
use std::time::Duration;

use pub_sub_client::PubSubClient;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;
use testcontainers::clients::Cli;
use testcontainers::images::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};

const TEST_TOPIC: &str = "projects/test/topics/test";
const TEST_SUBSCRIPTION: &str = "projects/test/subscriptions/test";

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
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

    // We interact with Pub/Sub via HTTP
    let reqwest_client = Client::new();

    // Create topic
    let response = reqwest_client
        .put(format!("{}/v1/{}", base_url, TEST_TOPIC))
        .send()
        .await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Create subscription
    let response = reqwest_client
        .put(format!("{}/v1/{}", base_url, TEST_SUBSCRIPTION))
        .json(&json!({ "topic": TEST_TOPIC }))
        .send()
        .await;
    assert!(response.is_ok());
    let response = response.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Publish to topic
    let data = base64::encode(json!({"text": "test"}).to_string());
    let response = reqwest_client
        .post(format!("{}/v1/{}:publish", base_url, TEST_TOPIC))
        .json(&json!(
          {
            "messages": [
              {
                "data": data,
                "attributes": {
                  "type": "Foo"
                }
              },
              {
                "data": data,
                "attributes": {
                  "type": "Bar"
                }
              }
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
        "examples/akka-serverless-playground-cea91d34cf9e.json",
        Duration::from_secs(30),
    );
    env::set_var("PUB_SUB_BASE_URL", "");
    assert!(pub_sub_client.is_ok());
    let pub_sub_client = pub_sub_client.unwrap();

    // Pull and verify
    let pull_response = pub_sub_client
        .pull_insert_attribute::<Message>(TEST_SUBSCRIPTION, 42, "type")
        .await;
    assert!(pull_response.is_ok());
    let pull_response = pull_response.unwrap();
    assert_eq!(pull_response.len(), 2);

    assert!(pull_response[0].is_ok());
    assert_eq!(
        pull_response[0].as_ref().unwrap().message,
        Message::Foo {
            text: "test".to_string()
        }
    );

    assert!(pull_response[1].is_ok());
    assert_eq!(
        pull_response[1].as_ref().unwrap().message,
        Message::Bar {
            text: "test".to_string()
        }
    );
}
