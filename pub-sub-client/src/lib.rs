mod error;
mod publisher;
mod subscriber;

pub use error::*;
pub use publisher::*;
pub use subscriber::*;

#[cfg(feature = "derive")]
pub use pub_sub_client_derive::*;

use goauth::auth::JwtClaims;
use goauth::credentials::Credentials;
use goauth::fetcher::TokenFetcher;
use goauth::scopes::Scope;
use reqwest::Response;
use serde::Serialize;
use smpl_jwt::Jwt;
use std::env;
use std::time::Duration;

const BASE_URL_ENV_VAR: &str = "PUB_SUB_BASE_URL";
const DEFAULT_BASE_URL: &str = "https://pubsub.googleapis.com";

pub struct PubSubClient {
    project_url: String,
    token_fetcher: TokenFetcher,
    reqwest_client: reqwest::Client,
}

impl std::fmt::Debug for PubSubClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubClient")
            .field("project_url", &self.project_url)
            .finish()
    }
}

impl PubSubClient {
    pub fn new(key_path: &str, refresh_buffer: Duration) -> Result<Self, Error> {
        let credentials =
            Credentials::from_file(key_path).map_err(|source| Error::Initialization {
                reason: format!("Missing or malformed service account key at `{key_path}`"),
                source: source.into(),
            })?;

        let base_url = env::var(BASE_URL_ENV_VAR).unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let project_id = credentials.project();
        let project_url = format!("{base_url}/v1/projects/{project_id}");

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
                    reason: format!("Malformed private key in service account key at `{key_path}`"),
                    source: source.into(),
                })?,
            None,
        );

        let refresh_buffer = refresh_buffer
            .try_into()
            .map_err(|source| Error::Initialization {
                reason: format!("Invalid refresh_buffer `{refresh_buffer:?}`"),
                source: Box::new(source),
            })?;

        Ok(Self {
            project_url,
            token_fetcher: TokenFetcher::new(jwt, credentials, refresh_buffer),
            reqwest_client: reqwest::Client::new(),
        })
    }

    async fn send_request<R: Serialize>(
        &self,
        url: &str,
        request: &R,
        timeout: Option<Duration>,
    ) -> Result<Response, Error> {
        let token = self
            .token_fetcher
            .fetch_token()
            .await
            .map_err(|source| Error::TokenFetch { source })?;

        let request = self
            .reqwest_client
            .post(url)
            .bearer_auth(token.access_token())
            .json(request);
        let request = timeout.into_iter().fold(request, |r, t| r.timeout(t));

        request
            .send()
            .await
            .map_err(|source| Error::HttpServiceCommunication { source })
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, PubSubClient};
    use serde::Deserialize;
    use std::time::Duration;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    enum Message {
        Foo { text: String },
        Bar { text: String },
    }

    #[test]
    fn test_new_ok() {
        let result = PubSubClient::new("tests/valid_key.json", Duration::from_secs(30));
        assert!(result.is_ok());
    }

    #[test]
    fn test_new_err_non_existent_key() {
        let result = PubSubClient::new("non_existent", Duration::from_secs(30));
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Initialization {
                reason: _,
                source: _,
            } => (),
            other => panic!("Expected Error::InvalidServiceAccountKey, but was `{other}`"),
        }
    }

    #[test]
    fn test_new_err_invalid_key() {
        let result = PubSubClient::new("Cargo.toml", Duration::from_secs(30));
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Initialization {
                reason: _,
                source: _,
            } => (),
            other => panic!("Expected Error::InvalidServiceAccountKey, but was `{other}`"),
        }
    }

    #[test]
    fn test_new_err_invalid_private_key() {
        let result = PubSubClient::new("tests/invalid_key.json", Duration::from_secs(30));
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Initialization {
                reason: _,
                source: _,
            } => (),
            other => panic!("Expected Error::InvalidPrivateKey, but was `{other}`"),
        }
    }
}
