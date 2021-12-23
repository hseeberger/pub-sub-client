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
use std::time::Duration;

pub struct PubSubClient {
    project_id: String,
    token_fetcher: TokenFetcher,
    reqwest_client: reqwest::Client,
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
    #[error("Cannot transform JSON value, because {0}")]
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
    pub fn new(key_path: String, refresh_buffer: Duration) -> Result<Self, Error> {
        let credentials = Credentials::from_file(&key_path)
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
            project_id: credentials.project(),
            token_fetcher: TokenFetcher::new(jwt, credentials, refresh_buffer),
            reqwest_client: reqwest::Client::new(),
        })
    }

    pub async fn pull<M: DeserializeOwned>(
        &self,
        subscription_id: String,
        max_messages: u32,
    ) -> Result<Vec<Result<MessageEnvelope<M>, Error>>, Error> {
        self.pull_with_transform(subscription_id, max_messages, transform::identity)
            .await
    }

    pub async fn pull_insert_attribute<M: DeserializeOwned>(
        &self,
        subscription_id: String,
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
        subscription_id: String,
        max_messages: u32,
        transform: F,
    ) -> Result<Vec<Result<MessageEnvelope<M>, Error>>, Error>
    where
        M: DeserializeOwned,
        F: Fn(&ReceivedMessage, Value) -> Result<Value, Error>,
    {
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}:pull",
            self.project_id, subscription_id
        );
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

        let messages = pull_response
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
            .collect();
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 42);
    }
}
