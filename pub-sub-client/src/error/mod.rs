use reqwest::Response;
use serde_json::Value;
use std::error::Error as StdError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Initialization error: {reason}")]
    Initialization {
        reason: String,
        source: Box<dyn StdError>,
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
    #[error("PubSubMessage contains no data")]
    NoData,
    #[error("Deserializing data of received message failed")]
    Deserialize { source: serde_json::Error },
    #[error("Serializing of message to be published failed")]
    Serialize { source: serde_json::Error },
    #[error("Failed to transform JSON value")]
    Transform {
        source: Box<dyn StdError + Send + Sync + 'static>,
    },
}

impl Error {
    pub async fn unexpected_http_status_code(response: Response) -> Error {
        Error::UnexpectedHttpStatusCode(
            response.status(),
            response
                .text()
                .await
                .map_err(|e| format!("Failed to get response body as text: {e}"))
                .and_then(|text| {
                    serde_json::from_str::<Value>(&text)
                        .map(|v| v["error"]["message"].to_string())
                        .map_err(|e| format!("Failed to parse error response: {e}"))
                })
                .unwrap(),
        )
    }
}
