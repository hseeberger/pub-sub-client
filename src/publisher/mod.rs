use crate::{error::Error, PubSubClient};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, time::Duration};
use tracing::debug;

pub struct PublishedMessageEnvelope<M>
where
    M: Serialize,
{
    message: M,
    attributes: Option<HashMap<String, String>>,
}

impl<M> From<M> for PublishedMessageEnvelope<M>
where
    M: Serialize,
{
    fn from(message: M) -> Self {
        Self {
            message,
            attributes: None,
        }
    }
}

impl<M> From<(M, HashMap<String, String>)> for PublishedMessageEnvelope<M>
where
    M: Serialize,
{
    fn from((message, attributes): (M, HashMap<String, String>)) -> Self {
        Self {
            message,
            attributes: Some(attributes),
        }
    }
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RawPublishedMessage<'a> {
    pub data: Option<String>,
    pub attributes: Option<HashMap<String, String>>,
    pub ordering_key: Option<&'a str>,
}

impl<'a> RawPublishedMessage<'a> {
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

    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
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
    messages: Vec<RawPublishedMessage<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishResponse {
    message_ids: Vec<String>,
}

impl PubSubClient {
    #[tracing::instrument]
    pub async fn publish<M, E>(
        &self,
        topic_id: &str,
        envelopes: Vec<E>,
        ordering_key: Option<&'_ str>,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Error>
    where
        M: Serialize,
        E: Into<PublishedMessageEnvelope<M>> + Debug,
    {
        let bytes = envelopes
            .into_iter()
            .map(|envelope| {
                let PublishedMessageEnvelope {
                    message,
                    attributes,
                } = envelope.into();
                serde_json::to_vec(&message).map(|bytes| (bytes, attributes))
            })
            .collect::<Result<Vec<_>, _>>();

        let messages = bytes
            .map_err(Error::Serialize)?
            .into_iter()
            .map(|(bytes, attributes)| RawPublishedMessage {
                data: Some(STANDARD.encode(bytes)),
                attributes,
                ordering_key,
            })
            .collect::<Vec<_>>();

        self.publish_raw(topic_id, messages, timeout).await
    }

    #[tracing::instrument]
    pub async fn publish_raw(
        &self,
        topic_id: &str,
        messages: Vec<RawPublishedMessage<'_>>,
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
            .map_err(Error::UnexpectedHttpResponse)?
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
