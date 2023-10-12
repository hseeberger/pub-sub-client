use reqwest::Response;
use serde_json::Value;
use std::{convert::identity, error::Error as StdError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("initialization error: {reason}")]
    Initialization {
        reason: String,
        source: Box<dyn StdError + Send + Sync + 'static>,
    },

    #[error("getting authentication token failed")]
    TokenFetch(#[from] Box<goauth::GoErr>),

    #[error("HTTP communication with Pub/Sub service failed")]
    HttpServiceCommunication(#[source] reqwest::Error),
    #[error("unexpected HTTP status code `{0}` from Pub/Sub service: {1}")]
    UnexpectedHttpStatusCode(reqwest::StatusCode, String),
    #[error("unexpected HTTP response from Pub/Sub service")]
    UnexpectedHttpResponse(#[source] reqwest::Error),

    #[error("decoding data of received message as Base64 failed")]
    DecodeBase64(#[source] base64::DecodeError),
    #[error("message contains no data")]
    NoData,
    #[error("deserializing data of received message failed")]
    Deserialize(#[source] serde_json::Error),
    #[error("serializing of message to be published failed")]
    Serialize(#[source] serde_json::Error),
    #[error("failed to transform JSON value")]
    Transform(#[source] Box<dyn StdError + Send + Sync + 'static>),
}

impl Error {
    pub async fn unexpected_http_status_code(response: Response) -> Error {
        Error::UnexpectedHttpStatusCode(
            response.status(),
            response
                .text()
                .await
                .map_err(|e| format!("failed to get response body as text: {e}"))
                .and_then(|text| {
                    serde_json::from_str::<Value>(&text)
                        .map_err(|e| format!("failed to parse error response: {e}"))
                        .map(|v| v["error"]["message"].to_string())
                })
                .unwrap_or_else(identity),
        )
    }
}
