use crate::error::Error;
use crate::PubSubClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use tracing::debug;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubMessage {
    pub data: Option<String>,
    pub attributes: HashMap<String, String>,
    pub ordering_key: Option<String>,
}

impl PubSubMessage {
    pub fn from_data(data: String) -> Self {
        Self {
            data: Some(data),
            attributes: HashMap::new(),
            ordering_key: None,
        }
    }

    pub fn from_attributes(attributes: HashMap<String, String>) -> Self {
        Self {
            data: None,
            attributes,
            ordering_key: None,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublishRequest {
    messages: Vec<PubSubMessage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishResponse {
    message_ids: Vec<String>,
}

impl PubSubClient {
    #[tracing::instrument]
    pub async fn publish<M: Serialize + Debug>(
        &self,
        topic_id: &str,
        messages: Vec<M>,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Error> {
        // Turn collection of results into reult of collection, failing fast;
        // see https://doc.rust-lang.org/std/result/enum.Result.html#impl-FromIterator%3CResult%3CA%2C%20E%3E%3E
        let messages: Result<Vec<_>, _> = messages.iter().map(|m| serde_json::to_vec(m)).collect();
        let messages = messages
            .map_err(|source| Error::Serialize { source })?
            .iter()
            .map(|bytes| PubSubMessage {
                data: Some(base64::encode(bytes)),
                attributes: HashMap::new(),
                ordering_key: None,
            })
            .collect::<Vec<_>>();

        self.publish_raw(topic_id, messages, timeout).await
    }

    #[tracing::instrument]
    pub async fn publish_raw(
        &self,
        topic_id: &str,
        messages: Vec<PubSubMessage>,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Error> {
        let url = self.topic_url(topic_id);
        let request = PublishRequest { messages };
        debug!(message = "Sending request", url = display(&url));
        let response = self.send_request(&url, &request, timeout).await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code(response).await);
        }

        let message_ids = response
            .json::<PublishResponse>()
            .await
            .map_err(|source| Error::UnexpectedHttpResponse { source })?
            .message_ids;
        debug!(
            message = "Request was successful",
            message_ids = debug(&message_ids)
        );
        Ok(message_ids)
    }

    fn topic_url(&self, topic_id: &str) -> String {
        let project_url = &self.project_url;
        format!("{project_url}/topics/{topic_id}:publish")
    }
}
