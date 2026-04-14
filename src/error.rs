use reqwest::Response;
use serde_json::Value;
use std::{convert::identity, error::Error as StdError};
use thiserror::Error;

/// Errors that can occur while using a [`PubSubClient`](crate::PubSubClient).
#[derive(Debug, Error)]
pub enum Error {
    /// Creating the client failed, e.g. because the service account key file is missing or
    /// malformed.
    #[error("initialization error: {reason}")]
    Initialization {
        reason: String,
        source: Box<dyn StdError + Send + Sync + 'static>,
    },

    /// Fetching an authentication token from Google failed.
    #[error("getting authentication token failed")]
    TokenFetch(#[from] Box<goauth::GoErr>),

    /// Sending a request to the Pub/Sub service failed.
    #[error("HTTP communication with Pub/Sub service failed")]
    HttpServiceCommunication(#[source] reqwest::Error),
    /// The Pub/Sub service responded with an unexpected (non-success) HTTP status code.
    #[error("unexpected HTTP status code `{0}` from Pub/Sub service: {1}")]
    UnexpectedHttpStatusCode(reqwest::StatusCode, String),
    /// The body of a successful Pub/Sub response could not be decoded as expected.
    #[error("unexpected HTTP response from Pub/Sub service")]
    UnexpectedHttpResponse(#[source] reqwest::Error),

    /// Base64 decoding the data of a received message failed.
    #[error("decoding data of received message as Base64 failed")]
    DecodeBase64(#[source] base64::DecodeError),
    /// A received message contained no data.
    #[error("message contains no data")]
    NoData,
    /// Deserializing the data of a received message into a domain message failed.
    #[error("deserializing data of received message failed")]
    Deserialize(#[source] serde_json::Error),
    /// Serializing a domain message to be published failed.
    #[error("serializing of message to be published failed")]
    Serialize(#[source] serde_json::Error),
    /// The user supplied transform for a pulled JSON value failed.
    #[error("failed to transform JSON value")]
    Transform(#[source] Box<dyn StdError + Send + Sync + 'static>),
}

impl Error {
    /// Build an [`Error::UnexpectedHttpStatusCode`] from a non-success `response`, extracting the
    /// Pub/Sub error message from the response body if possible.
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
