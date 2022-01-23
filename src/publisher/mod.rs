use crate::error::Error;
use crate::PubSubClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubMessage {
    pub data: String,
    pub attributes: HashMap<String, String>,
    pub ordering_key: Option<String>,
}

impl PubSubMessage {
    pub fn new(data: String) -> Self {
        Self {
            data,
            attributes: HashMap::new(),
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
    pub async fn publish<M: Serialize>(
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
                data: base64::encode(bytes),
                attributes: HashMap::new(),
                ordering_key: None,
            })
            .collect::<Vec<_>>();

        self.publish_raw(topic_id, messages, timeout).await
    }

    pub async fn publish_raw(
        &self,
        topic_id: &str,
        messages: Vec<PubSubMessage>,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Error> {
        let request = PublishRequest { messages };
        let response = self
            .send_request(&self.topic_url(topic_id), &request, timeout)
            .await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code(response).await);
        }

        let message_ids = response
            .json::<PublishResponse>()
            .await
            .map_err(|source| Error::UnexpectedHttpResponse { source })?
            .message_ids;
        Ok(message_ids)
    }

    fn topic_url(&self, topic_id: &str) -> String {
        let project_url = &self.project_url;
        format!("{project_url}/topics/{topic_id}:publish")
    }
}
