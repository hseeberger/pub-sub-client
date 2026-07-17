use crate::{PubSubClient, error::Error};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, time::Duration};
use tracing::debug;

/// A domain message to be published together with optional attributes.
///
/// Created via the [`From`] conversions for a bare message `M` (without attributes) or a
/// `(M, HashMap<String, String>)` tuple (with attributes), so [`PubSubClient::publish`] can be
/// called with either.
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

/// A raw message to be published, i.e. with already Base64 encoded `data`, optional `attributes`
/// and an optional `ordering_key`.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RawPublishedMessage<'a> {
    /// The already Base64 encoded message data.
    pub data: Option<String>,
    /// Optional attributes attached to the message.
    pub attributes: Option<HashMap<String, String>>,
    /// Optional key by which Pub/Sub orders messages.
    pub ordering_key: Option<&'a str>,
}

impl<'a> RawPublishedMessage<'a> {
    /// Create a new `RawPublishedMessage` with the given Base64 encoded `data` and no attributes or
    /// ordering key.
    pub fn new(data: String) -> Self {
        Self {
            data: Some(data),
            attributes: None,
            ordering_key: None,
        }
    }

    /// Set the Base64 encoded `data`.
    pub fn with_data(mut self, data: String) -> Self {
        self.data = Some(data);
        self
    }

    /// Set the `attributes`.
    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = Some(attributes);
        self
    }

    /// Set the `ordering_key`.
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
    /// Serialize the given message envelopes to JSON and publish them to the topic with the given
    /// ID, optionally using the given `ordering_key` and request `timeout`, returning the IDs of
    /// the published messages.
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

    /// Publish the given raw messages to the topic with the given ID, optionally using the given
    /// request `timeout`, returning the IDs of the published messages.
    #[tracing::instrument]
    pub async fn publish_raw(
        &self,
        topic_id: &str,
        messages: Vec<RawPublishedMessage<'_>>,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Error> {
        let url = format!("{}:publish", self.topic_url(topic_id));
        let request = PublishRequest { messages };
        let response = self
            .send_request(Method::POST, &url, &request, timeout)
            .await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code(response).await);
        }

        let message_ids = response
            .json::<PublishResponse>()
            .await
            .map_err(Error::UnexpectedHttpResponse)?
            .message_ids;
        debug!(?message_ids, "successfully published");
        Ok(message_ids)
    }
}
