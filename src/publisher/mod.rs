use crate::error::Error;
use crate::PubSubClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use tracing::debug;

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubMessage<'a> {
    pub data: Option<String>,
    pub attributes: Option<&'a HashMap<String, String>>,
    pub ordering_key: Option<&'a str>,
}

impl<'a> PubSubMessage<'a> {
    pub fn new(data: String) -> Self {
        Self {
            data: Some(data),
            attributes: None,
            ordering_key: None,
        }
    }

    pub fn with_data(mut self, data: String) -> Self {
        self.data = Some(data);
        self
    }

    pub fn with_attributes(mut self, attributes: &'a HashMap<String, String>) -> Self {
        self.attributes = Some(attributes);
        self
    }

    pub fn with_ordering_key(mut self, ordering_key: &'a str) -> Self {
        self.ordering_key = Some(ordering_key);
        self
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublishRequest<'a> {
    messages: Vec<PubSubMessage<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishResponse {
    message_ids: Vec<String>,
}

impl PubSubClient {
    #[tracing::instrument]
    pub async fn publish<'a, M: Serialize + Debug>(
        &self,
        topic_id: &str,
        messages: Vec<M>,
        attributes: Option<&'a HashMap<String, String>>,
        ordering_key: Option<&'a str>,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Error> {
        let bytes = messages
            .iter()
            .map(|m| serde_json::to_vec(m))
            .collect::<Result<Vec<_>, _>>();

        let messages = bytes
            .map_err(|source| Error::Serialize { source })?
            .iter()
            .map(|bytes| PubSubMessage {
                data: Some(base64::encode(bytes)),
                attributes: attributes,
                ordering_key: ordering_key,
            })
            .collect::<Vec<_>>();

        self.publish_raw(topic_id, messages, timeout).await
    }

    #[tracing::instrument]
    pub async fn publish_raw<'a>(
        &self,
        topic_id: &str,
        messages: Vec<PubSubMessage<'a>>,
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
