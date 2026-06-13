use assert_matches::assert_matches;
use base64::{Engine, engine::general_purpose::STANDARD};
use pub_sub_client::{PubSubClient, RawPublishedMessage};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::HashMap, env, error::Error, time::Duration, vec};
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};

const TOPIC_ID: &str = "test-topic";
const SUBSCRIPTION_ID: &str = "test-sub";
const TEXT: &str = "test-message";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum Message {
    Foo { text: String },
    Bar { text: String },
}

#[tokio::test]
async fn test() -> Result<(), Box<dyn Error>> {
    // Set up Pub/Sub. The `google/cloud-sdk` emulator image is only published for `linux/amd64`,
    // so pin the platform to run it (via emulation) on other architectures too.
    let pubsub = CloudSdk::pubsub()
        .with_platform("linux/amd64")
        .start()
        .await?;
    let pubsub_port = pubsub.get_host_port_ipv4(PUBSUB_PORT).await?;
    let base_url = format!("http://localhost:{pubsub_port}");

    // Create PubSubClient.
    // Notice: GitHub Actions write the `GCP_SERVICE_ACCOUNT` secret to the key path given by the
    // `SERVICE_ACCOUNT_PATH` env var.
    let service_account_path = env::var("SERVICE_ACCOUNT_PATH")?;
    unsafe {
        env::set_var("PUB_SUB_BASE_URL", base_url);
    }
    let pub_sub_client = PubSubClient::new(service_account_path, Duration::from_secs(30));
    unsafe {
        env::set_var("PUB_SUB_BASE_URL", "");
    }
    assert!(pub_sub_client.is_ok());
    let pub_sub_client = pub_sub_client.unwrap();

    // Create topic and subscription.
    assert!(pub_sub_client.create_topic(TOPIC_ID, None).await.is_ok());
    assert!(
        pub_sub_client
            .create_subscription(SUBSCRIPTION_ID, TOPIC_ID, None)
            .await
            .is_ok()
    );

    // Publish raw
    let foo = STANDARD.encode(json!({ "Foo": { "text": TEXT } }).to_string());
    let messages = vec![RawPublishedMessage::new(foo)];
    let response = pub_sub_client
        .publish_raw(TOPIC_ID, messages, Some(Duration::from_secs(10)))
        .await;
    assert_matches!(response, Ok(response) if response.len() == 1);

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
        .publish::<Message, _>(TOPIC_ID, messages, None, Some(Duration::from_secs(10)))
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

    Ok(())
}
