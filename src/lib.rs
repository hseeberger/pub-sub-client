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
    pub fn new(key_path: impl AsRef<str>, refresh_buffer: Duration) -> Result<Self, Error> {
        let key_path = key_path.as_ref();

        let credentials =
            Credentials::from_file(key_path).map_err(|error| Error::Initialization {
                reason: format!("missing or malformed service account key at `{key_path}`"),
                source: error.into(),
            })?;
        println!("{credentials:?}");

        let base_url = env::var(BASE_URL_ENV_VAR).unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let project_id = credentials.project();
        let project_url = format!("{base_url}/v1/projects/{project_id}");

        let jwt = Jwt::new(
            JwtClaims::new(
                credentials.iss(),
                &[Scope::PubSub],
                credentials.token_uri(),
                None,
                None,
            ),
            credentials
                .rsa_key()
                .map_err(|error| Error::Initialization {
                    reason: format!("malformed private key in service account key at `{key_path}`"),
                    source: error.into(),
                })?,
            None,
        );

        Ok(Self {
            project_url,
            token_fetcher: TokenFetcher::new(jwt, credentials, refresh_buffer.as_secs() as i64),
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
        let token = self.token_fetcher.fetch_token().map_err(Box::new)?;

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
    use assert_matches::assert_matches;
    use std::time::Duration;

    #[test]
    fn test_new_err_non_existent_key() {
        let result = PubSubClient::new("non_existent", Duration::from_secs(30));
        assert_matches!(result, Err(Error::Initialization { .. }));
    }

    #[test]
    fn test_new_err_invalid_key() {
        let result = PubSubClient::new("Cargo.toml", Duration::from_secs(30));
        assert_matches!(result, Err(Error::Initialization { .. }));
    }

    #[test]
    fn test_new_err_invalid_private_key() {
        let result = PubSubClient::new("tests/invalid_key.json", Duration::from_secs(30));
        assert_matches!(result, Err(Error::Initialization { .. }));
    }
}
