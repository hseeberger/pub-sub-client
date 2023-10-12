mod error;
mod publisher;
mod subscriber;

pub use error::*;
pub use publisher::*;
pub use subscriber::*;

use goauth::{auth::JwtClaims, credentials::Credentials, fetcher::TokenFetcher, scopes::Scope};
use reqwest::Response;
use serde::Serialize;
use smpl_jwt::Jwt;
use std::{
    env,
    fmt::{self, Debug, Formatter},
    time::Duration,
};

const BASE_URL_ENV_VAR: &str = "PUB_SUB_BASE_URL";
const DEFAULT_BASE_URL: &str = "https://pubsub.googleapis.com";

pub struct PubSubClient {
    project_url: String,
    token_fetcher: TokenFetcher,
    reqwest_client: reqwest::Client,
}

impl PubSubClient {
    pub fn new<T>(key_path: T, refresh_buffer: Duration) -> Result<Self, Error>
    where
        T: AsRef<str>,
    {
        let key_path = key_path.as_ref();
        let credentials =
            Credentials::from_file(key_path).map_err(|source| Error::Initialization {
                reason: format!("missing or malformed service account key at `{key_path}`"),
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
                    reason: format!("malformed private key in service account key at `{key_path}`"),
                    source: source.into(),
                })?,
            None,
        );

        let refresh_buffer = refresh_buffer
            .try_into()
            .map_err(|source| Error::Initialization {
                reason: format!("invalid refresh_buffer `{refresh_buffer:?}`"),
                source: Box::new(source),
            })?;

        Ok(Self {
            project_url,
            token_fetcher: TokenFetcher::new(jwt, credentials, refresh_buffer),
            reqwest_client: reqwest::Client::new(),
        })
    }

    async fn send_request<R>(
        &self,
        url: &str,
        request: &R,
        timeout: Option<Duration>,
    ) -> Result<Response, Error>
    where
        R: Serialize,
    {
        let token = self.token_fetcher.fetch_token().await.map_err(Box::new)?;

        let request = self
            .reqwest_client
            .post(url)
            .bearer_auth(token.access_token())
            .json(request);
        let request = timeout.into_iter().fold(request, |r, t| r.timeout(t));

        request
            .send()
            .await
            .map_err(Error::HttpServiceCommunication)
    }
}

impl Debug for PubSubClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSubClient")
            .field("project_url", &self.project_url)
            .finish()
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
