use goauth::auth::JwtClaims;
use goauth::credentials::Credentials;
use goauth::fetcher::TokenFetcher;
use goauth::scopes::Scope;
use reqwest::Response;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use smpl_jwt::Jwt;
use std::collections::HashMap;
use std::env;
use std::time::Duration;

const BASE_URL_ENV_VAR: &str = "PUB_SUB_BASE_URL";
const DEFAULT_BASE_URL: &str = "https://pubsub.googleapis.com";

pub struct PubSubClient {
    base_url: String,
    project_id: String,
    token_fetcher: TokenFetcher,
    reqwest_client: reqwest::Client,
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
    #[error("Initialization error: {reason}")]
    Initialization {
        reason: String,
        source: goauth::GoErr,
    },

    #[error("Getting authentication token failed")]
    TokenFetch { source: goauth::GoErr },

    #[error("HTTP communication with Pub/Sub service failed")]
    HttpServiceCommunication { source: reqwest::Error },
    #[error("Unexpected HTTP status code `{0}` from Pub/Sub service: {1}")]
    UnexpectedHttpStatusCode(reqwest::StatusCode, String),
    #[error("Unexpected HTTP response from Pub/Sub service")]
    UnexpectedHttpResponse { source: reqwest::Error },

    #[error("Decoding data of received message as Base64 failed")]
    NoBase64 { source: base64::DecodeError },
    #[error("Deserializing data of received message failed")]
    Deserialize { source: serde_json::Error },
    #[error("Failed to transform JSON value: {reason}")]
    Transform { reason: String },
}

impl Error {
    async fn unexpected_http_status_code_from_response(response: Response) -> Error {
        Error::UnexpectedHttpStatusCode(
            response.status(),
            response
                .text()
                .await
                .map(|e| {
                    serde_json::from_str::<Value>(&e).unwrap_or(Value::Null)["error"]["message"]
                        .to_string()
                })
                .unwrap_or_else(|_| "Failed to get text for HTTP body".to_string()),
        )
    }
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AcknowledgeRequest<'a> {
    ack_ids: Vec<&'a str>,
}

impl PubSubClient {
    pub fn new(key_path: &str, refresh_buffer: Duration) -> Result<Self, Error> {
        let credentials =
            Credentials::from_file(key_path).map_err(|source| Error::Initialization {
                reason: format!(
                    "Missing or malformed service account key file at `{}`",
                    key_path
                ),
                source,
            })?;

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
                .map_err(|source| Error::Initialization {
                    reason: format!(
                        "Malformed private key as part of service account key file at `{}`",
                        key_path
                    ),
                    source,
                })?,
            None,
        );

        // We do not want time::Duration to unnecessarily be exposed in our API
        let refresh_buffer = time::Duration::new(
            refresh_buffer.as_secs() as i64,
            refresh_buffer.as_nanos() as i32,
        );

        Ok(Self {
            base_url: env::var(BASE_URL_ENV_VAR).unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            project_id: credentials.project(),
            token_fetcher: TokenFetcher::new(jwt, credentials, refresh_buffer),
            reqwest_client: reqwest::Client::new(),
        })
    }

    pub async fn pull<M: DeserializeOwned>(
        &self,
        subscription_id: &str,
        max_messages: u32,
    ) -> Result<Vec<Result<MessageEnvelope<M>, Error>>, Error> {
        self.pull_with_transform(subscription_id, max_messages, |_, value| Ok(value))
            .await
    }

    pub async fn pull_insert_attribute<M: DeserializeOwned>(
        &self,
        subscription_id: &str,
        max_messages: u32,
        key: &str,
    ) -> Result<Vec<Result<MessageEnvelope<M>, Error>>, Error> {
        self.pull_with_transform(subscription_id, max_messages, |received_message, value| {
            insert_attribute(key, received_message, value)
        })
        .await
    }

    pub async fn pull_with_transform<M, T>(
        &self,
        subscription_id: &str,
        max_messages: u32,
        transform: T,
    ) -> Result<Vec<Result<MessageEnvelope<M>, Error>>, Error>
    where
        M: DeserializeOwned,
        T: Fn(&ReceivedMessage, Value) -> Result<Value, Error>,
    {
        let url = format!(
            "{}/v1/projects/{}/subscriptions/{}:pull",
            self.base_url, self.project_id, subscription_id
        );
        let request = PullRequest { max_messages };
        let response = self.send_request(&url, &request).await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code_from_response(response).await);
        }

        let pull_response = response
            .json::<PullResponse>()
            .await
            .map_err(|source| Error::UnexpectedHttpResponse { source })?;

        let messages = messages_from_pull_response(pull_response, transform);
        Ok(messages)
    }

    /// According to how Google Cloud Pub/Sub works, passing at least one invalid ACK ID fails the
    /// whole request via a 400 Bad Request response.
    pub async fn acknowledge(
        &self,
        subscription_id: &str,
        ack_ids: Vec<&str>,
    ) -> Result<(), Error> {
        let url = format!(
            "{}/v1/projects/{}/subscriptions/{}:acknowledge",
            self.base_url, self.project_id, subscription_id
        );
        let request = AcknowledgeRequest { ack_ids };
        let response = self.send_request(&url, &request).await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code_from_response(response).await);
        }

        Ok(())
    }

    async fn send_request<R: Serialize>(&self, url: &str, request: &R) -> Result<Response, Error> {
        let token = self
            .token_fetcher
            .fetch_token()
            .await
            .map_err(|source| Error::TokenFetch { source })?;
        let response = self
            .reqwest_client
            .post(url)
            .bearer_auth(token.access_token())
            .json(request)
            .send()
            .await
            .map_err(|source| Error::HttpServiceCommunication { source })?;
        Ok(response)
    }
}

fn messages_from_pull_response<M, T>(
    response: PullResponse,
    transform: T,
) -> Vec<Result<MessageEnvelope<M>, Error>>
where
    M: DeserializeOwned,
    T: Fn(&ReceivedMessage, Value) -> Result<Value, Error>,
{
    response
        .received_messages
        .into_iter()
        .map(|received_message| {
            base64::decode(&received_message.message.data)
                .map_err(|source| Error::NoBase64 { source })
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

pub fn insert_attribute(
    key: &str,
    received_message: &ReceivedMessage,
    value: Value,
) -> Result<Value, Error> {
    match received_message.message.attributes.get(key) {
        Some(v) => match value {
            Value::Object(mut map) => {
                map.insert(key.to_string(), json!(v));
                Ok(Value::Object(map))
            }
            other => Err(Error::Transform {
                reason: format!("Unexpected JSON value `{}`", other),
            }),
        },
        None => Err(Error::Transform {
            reason: format!("Missing attribute `{}`", key),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        insert_attribute, messages_from_pull_response, Error, MessageEnvelope, PubSubClient,
        PubSubMessage, PullResponse, ReceivedMessage,
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
            Error::Initialization {
                reason: _,
                source: _,
            } => (),
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
            Error::Initialization {
                reason: _,
                source: _,
            } => (),
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
            Error::Initialization {
                reason: _,
                source: _,
            } => (),
            other => panic!("Expected Error::InvalidPrivateKey, but was {}", other),
        }
    }

    #[test]
    fn messages_from_pull_response_ok() {
        let received_messages = vec![ReceivedMessage {
            ack_id: "ack_id".to_string(),
            message: PubSubMessage {
                id: "id".to_string(),
                data: base64::encode(json!({"text": "test"}).to_string()),
                attributes: HashMap::from([("type".to_string(), "Foo".to_string())]),
            },
            delivery_attempt: 1,
        }];
        let pull_response = PullResponse { received_messages };
        let mut messages_result: Vec<Result<MessageEnvelope<Message>, Error>> =
            messages_from_pull_response(pull_response, |received_message, value| {
                insert_attribute("type", received_message, value)
            });
        assert_eq!(messages_result.len(), 1);
        assert!(messages_result[0].is_ok());
        let envelope = messages_result.pop().unwrap().unwrap();
        assert_eq!(envelope.id, "id".to_string());
        assert_eq!(envelope.ack_id, "ack_id".to_string());
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
    fn messages_from_pull_response_error() {
        let received_messages = vec![ReceivedMessage {
            ack_id: "ack_id".to_string(),
            message: PubSubMessage {
                id: "id".to_string(),
                data: base64::encode(json!({"text": "test"}).to_string()),
                attributes: HashMap::new(),
            },
            delivery_attempt: 1,
        }];
        let pull_response = PullResponse { received_messages };
        let mut messages_result: Vec<Result<MessageEnvelope<Message>, Error>> =
            messages_from_pull_response(pull_response, |received_message, value| {
                insert_attribute("type", received_message, value)
            });
        assert_eq!(messages_result.len(), 1);
        assert!(messages_result[0].is_err());
        let error = messages_result.pop().unwrap().unwrap_err();
        assert_eq!(
            format!("{}", error),
            "Failed to transform JSON value: Missing attribute `type`"
        );
    }

    #[test]
    fn insert_attribute_ok() {
        let received_message = ReceivedMessage {
            ack_id: "ack_id".to_string(),
            message: PubSubMessage {
                id: "id".to_string(),
                data: String::new(),
                attributes: HashMap::from([("type".to_string(), "Foo".to_string())]),
            },
            delivery_attempt: 1,
        };
        let value = json!({"text": "test"});
        let new_value = insert_attribute("type", &received_message, value);
        assert!(new_value.is_ok());
        let new_value = new_value.unwrap();
        assert_eq!(new_value, json!({"text": "test", "type": "Foo"}));
    }

    #[test]
    fn insert_attribute_error_missing_attribute() {
        let received_message = ReceivedMessage {
            ack_id: "ack_id".to_string(),
            message: PubSubMessage {
                id: "id".to_string(),
                data: String::new(),
                attributes: HashMap::new(),
            },
            delivery_attempt: 1,
        };
        let value = json!({"text": "test"});
        let new_value = insert_attribute("type", &received_message, value);
        assert!(new_value.is_err());
        let error = new_value.unwrap_err();
        assert_eq!(
            format!("{}", error),
            "Failed to transform JSON value: Missing attribute `type`"
        );
    }

    #[test]
    fn insert_attribute_error_unexpected_value() {
        let received_message = ReceivedMessage {
            ack_id: "ack_id".to_string(),
            message: PubSubMessage {
                id: "id".to_string(),
                data: String::new(),
                attributes: HashMap::from([("type".to_string(), "Foo".to_string())]),
            },
            delivery_attempt: 1,
        };
        let value = json!(json!([]));
        let new_value = insert_attribute("type", &received_message, value);
        assert!(new_value.is_err());
        let error = new_value.unwrap_err();
        assert_eq!(
            format!("{}", error),
            "Failed to transform JSON value: Unexpected JSON value `[]`"
        );
    }
}
