pub mod transform;

use goauth::auth::JwtClaims;
use goauth::credentials::Credentials;
use goauth::fetcher::TokenFetcher;
use goauth::scopes::Scope;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smpl_jwt::Jwt;
use std::collections::HashMap;
use std::env;
use std::time::Duration;

const BASE_URL: &str = "PUB_SUB_BASE_URL";
const DEFAULT_BASE_URL: &str = "https://pubsub.googleapis.com";

pub struct PubSubClient {
    token_fetcher: TokenFetcher,
    reqwest_client: reqwest::Client,
    base_url: String,
}

impl std::fmt::Debug for PubSubClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubClient")
            .field("base_url", &self.base_url)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Missing or invalid service account key")]
    InvalidServiceAccountKey { source: goauth::GoErr },
    #[error("Invalid private key (as part of service account key)")]
    InvalidPrivateKey { source: goauth::GoErr },
    #[error("Failed to fetch token")]
    TokenFetch { source: goauth::GoErr },
    #[error("Failed to pull")]
    Pull { source: reqwest::Error },
    #[error("Unexpected HTTP status code `{0}`")]
    UnexpectedPullStatusCode(reqwest::StatusCode),
    #[error("Failed to parse HTTP response from Pub/Sub")]
    Parse { source: reqwest::Error },
    #[error("Failed to decode message as Base64")]
    Decode { source: base64::DecodeError },
    #[error("Failed to deserialize message")]
    Deserialize { source: serde_json::Error },
    #[error("Failed to transform JSON value: {0}")]
    Transform(String),
}

#[derive(Debug)]
pub struct MessageEnvelope<M: DeserializeOwned> {
    pub id: String,
    pub ack_id: String,
    pub attributes: HashMap<String, String>,
    pub delivery_attempt: u32,
    pub message: M,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceivedMessage {
    pub ack_id: String,
    pub message: PubSubMessage,
    #[serde(default)] // The Pub/Sub emulator does not send this field!
    pub delivery_attempt: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubMessage {
    #[serde(rename = "messageId")]
    pub id: String,
    pub data: String,
    #[serde(default = "HashMap::default")]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PullRequest {
    max_messages: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullResponse {
    #[serde(default = "Vec::default")]
    received_messages: Vec<ReceivedMessage>,
}

impl PubSubClient {
    pub fn new(key_path: &str, refresh_buffer: Duration) -> Result<Self, Error> {
        let credentials = Credentials::from_file(key_path)
            .map_err(|source| Error::InvalidServiceAccountKey { source })?;

        let jwt = Jwt::new(
            JwtClaims::new(
                credentials.iss(),
                &Scope::PubSub,
                credentials.token_uri(),
                None,
                None,
            ),
            credentials
                .rsa_key()
                .map_err(|source| Error::InvalidPrivateKey { source })?,
            None,
        );

        // We do not want time::Duration to unnecessarily be exposed in our API
        let refresh_buffer = time::Duration::new(
            refresh_buffer.as_secs() as i64,
            refresh_buffer.as_nanos() as i32,
        );

        Ok(Self {
            token_fetcher: TokenFetcher::new(jwt, credentials, refresh_buffer),
            reqwest_client: reqwest::Client::new(),
            base_url: env::var(BASE_URL).unwrap_or(DEFAULT_BASE_URL.to_string()),
        })
    }

    pub async fn pull<M: DeserializeOwned>(
        &self,
        subscription_id: &str,
        max_messages: u32,
    ) -> Result<Vec<Result<MessageEnvelope<M>, Error>>, Error> {
        self.pull_with_transform(subscription_id, max_messages, transform::identity)
            .await
    }

    pub async fn pull_insert_attribute<M: DeserializeOwned>(
        &self,
        subscription_id: &str,
        max_messages: u32,
        key: &str,
    ) -> Result<Vec<Result<MessageEnvelope<M>, Error>>, Error> {
        self.pull_with_transform(subscription_id, max_messages, |received_message, value| {
            transform::insert_attribute(key, received_message, value)
        })
        .await
    }

    pub async fn pull_with_transform<M, F>(
        &self,
        subscription: &str,
        max_messages: u32,
        transform: F,
    ) -> Result<Vec<Result<MessageEnvelope<M>, Error>>, Error>
    where
        M: DeserializeOwned,
        F: Fn(&ReceivedMessage, Value) -> Result<Value, Error>,
    {
        let url = format!("{}/v1/{}:pull", self.base_url, subscription);
        let token = self
            .token_fetcher
            .fetch_token()
            .await
            .map_err(|source| Error::TokenFetch { source })?;
        let pull_request = PullRequest { max_messages };
        let response = self
            .reqwest_client
            .post(&url)
            .bearer_auth(token.access_token())
            .json(&pull_request)
            .send()
            .await
            .map_err(|source| Error::Pull { source })?;
        if !response.status().is_success() {
            return Err(Error::UnexpectedPullStatusCode(response.status()));
        }

        let pull_response = response
            .json::<PullResponse>()
            .await
            .map_err(|source| Error::Parse { source })?;

        Ok(messages(pull_response, transform))
    }
}

fn messages<M, F>(
    pull_response: PullResponse,
    transform: F,
) -> Vec<Result<MessageEnvelope<M>, Error>>
where
    M: DeserializeOwned,
    F: Fn(&ReceivedMessage, Value) -> Result<Value, Error>,
{
    pull_response
        .received_messages
        .into_iter()
        .map(|received_message| {
            base64::decode(&received_message.message.data)
                .map_err(|source| Error::Decode { source })
                .and_then(|decoded_data| {
                    serde_json::from_slice::<Value>(&decoded_data)
                        .map_err(|source| Error::Deserialize { source })
                })
                .and_then(|value| transform(&received_message, value))
                .and_then(|transformed_value| {
                    serde_json::from_value(transformed_value)
                        .map_err(|source| Error::Deserialize { source })
                })
                .map(|message| MessageEnvelope {
                    id: received_message.message.id,
                    ack_id: received_message.ack_id,
                    attributes: received_message.message.attributes,
                    delivery_attempt: received_message.delivery_attempt,
                    message,
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::{
        messages, transform, Error, MessageEnvelope, PubSubClient, PubSubMessage, PullResponse,
        ReceivedMessage,
    };
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::HashMap;
    use std::time::Duration;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(tag = "type")]
    enum Message {
        Foo { text: String },
        Bar { text: String },
    }

    #[test]
    fn new_ok() {
        let result = PubSubClient::new("tests/valid_key.json", Duration::from_secs(30));
        assert!(result.is_ok());
    }

    #[test]
    fn new_err_non_existent_key() {
        let result = PubSubClient::new("non_existent", Duration::from_secs(30));
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidServiceAccountKey { source: _ } => (),
            other => panic!(
                "Expected Error::InvalidServiceAccountKey, but was {}",
                other
            ),
        }
    }

    #[test]
    fn new_err_invalid_key() {
        let result = PubSubClient::new("Cargo.toml", Duration::from_secs(30));
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidServiceAccountKey { source: _ } => (),
            other => panic!(
                "Expected Error::InvalidServiceAccountKey, but was {}",
                other
            ),
        }
    }

    #[test]
    fn new_err_invalid_private_key() {
        let result = PubSubClient::new("tests/invalid_key.json", Duration::from_secs(30));
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidPrivateKey { source: _ } => (),
            other => panic!("Expected Error::InvalidPrivateKey, but was {}", other),
        }
    }

    #[test]
    fn messages_ok() {
        let received_messages = vec![ReceivedMessage {
            ack_id: "ack_id_1".to_string(),
            message: PubSubMessage {
                id: "id_1".to_string(),
                data: base64::encode(json!({"text": "test"}).to_string()),
                attributes: HashMap::from([("type".to_string(), "Foo".to_string())]),
            },
            delivery_attempt: 1,
        }];
        let pull_response = PullResponse { received_messages };
        let mut messages_result: Vec<Result<MessageEnvelope<Message>, Error>> =
            messages(pull_response, |received_message, value| {
                transform::insert_attribute("type", received_message, value)
            });
        assert_eq!(messages_result.len(), 1);
        assert!(messages_result[0].is_ok());
        let envelope = messages_result.pop().unwrap().unwrap();
        assert_eq!(envelope.id, "id_1".to_string());
        assert_eq!(envelope.ack_id, "ack_id_1".to_string());
        assert_eq!(
            envelope.attributes,
            HashMap::from([("type".to_string(), "Foo".to_string())])
        );
        assert_eq!(
            envelope.message,
            Message::Foo {
                text: "test".to_string()
            }
        );
    }

    #[test]
    fn messages_error() {
        let received_messages = vec![ReceivedMessage {
            ack_id: "ack_id_2".to_string(),
            message: PubSubMessage {
                id: "id_2".to_string(),
                data: base64::encode(json!({"text": "test"}).to_string()),
                attributes: HashMap::new(),
            },
            delivery_attempt: 2,
        }];
        let pull_response = PullResponse { received_messages };
        let mut messages_result: Vec<Result<MessageEnvelope<Message>, Error>> =
            messages(pull_response, |received_message, value| {
                transform::insert_attribute("type", received_message, value)
            });
        assert_eq!(messages_result.len(), 1);
        assert!(messages_result[0].is_err());
        let error = messages_result.pop().unwrap().unwrap_err();
        assert_eq!(
            format!("{}", error),
            "Failed to transform JSON value: Missing attribute `type`"
        );
    }
}
