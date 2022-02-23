use crate::error::Error;
use crate::PubSubClient;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::time::Duration;
use time::OffsetDateTime;
use tracing::debug;

#[derive(Debug)]
pub struct PulledMessage<M: DeserializeOwned> {
    pub ack_id: String,
    pub message: Result<M, Error>,
    pub attributes: Option<HashMap<String, String>>,
    pub id: String,
    pub publish_time: OffsetDateTime,
    pub ordering_key: Option<String>,
    pub delivery_attempt: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawPulledMessageEnvelope {
    pub ack_id: String,
    pub message: RawPulledMessage,
    #[serde(default)] // The Pub/Sub emulator does not send this field!
    pub delivery_attempt: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawPulledMessage {
    pub data: Option<String>,
    pub attributes: Option<HashMap<String, String>>,
    #[serde(rename = "messageId")]
    pub id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub publish_time: OffsetDateTime,
    pub ordering_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PullRequest {
    max_messages: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullResponse {
    #[serde(default, rename = "receivedMessages")]
    envelopes: Vec<RawPulledMessageEnvelope>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AcknowledgeRequest<'a> {
    ack_ids: Vec<&'a str>,
}

impl PubSubClient {
    #[tracing::instrument]
    pub async fn pull<M: DeserializeOwned + Debug>(
        &self,
        subscription_id: &str,
        max_messages: u32,
        timeout: Option<Duration>,
    ) -> Result<Vec<PulledMessage<M>>, Error> {
        self.pull_with_transform(subscription_id, max_messages, timeout, |_, value| Ok(value))
            .await
    }

    #[tracing::instrument(skip(transform))]
    pub async fn pull_with_transform<M, T>(
        &self,
        subscription_id: &str,
        max_messages: u32,
        timeout: Option<Duration>,
        transform: T,
    ) -> Result<Vec<PulledMessage<M>>, Error>
    where
        M: DeserializeOwned + Debug,
        T: Fn(
            &RawPulledMessageEnvelope,
            Value,
        ) -> Result<Value, Box<dyn StdError + Send + Sync + 'static>>,
    {
        let envelopes = self
            .pull_raw(subscription_id, max_messages, timeout)
            .await?;
        let messages = deserialize(envelopes, transform);
        Ok(messages)
    }

    #[tracing::instrument]
    pub async fn pull_raw(
        &self,
        subscription_id: &str,
        max_messages: u32,
        timeout: Option<Duration>,
    ) -> Result<Vec<RawPulledMessageEnvelope>, Error> {
        let url = self.subscription_url(subscription_id, "pull");
        let request = PullRequest { max_messages };
        debug!(message = "Sending request", url = display(&url));
        let response = self.send_request(&url, &request, timeout).await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code(response).await);
        }

        let envelopes = response
            .json::<PullResponse>()
            .await
            .map_err(|source| Error::UnexpectedHttpResponse { source })?
            .envelopes;

        Ok(envelopes)
    }

    /// According to how Google Cloud Pub/Sub works, passing at least one invalid ACK ID fails the
    /// whole request via a 400 Bad Request response.
    pub async fn acknowledge(
        &self,
        subscription_id: &str,
        ack_ids: Vec<&str>,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        let request = AcknowledgeRequest { ack_ids };
        let response = self
            .send_request(
                &self.subscription_url(subscription_id, "acknowledge"),
                &request,
                timeout,
            )
            .await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code(response).await);
        }

        Ok(())
    }

    fn subscription_url(&self, subscription_id: &str, action: &str) -> String {
        let project_url = &self.project_url;
        format!("{project_url}/subscriptions/{subscription_id}:{action}")
    }
}

fn deserialize<M, T>(
    envelopes: Vec<RawPulledMessageEnvelope>,
    transform: T,
) -> Vec<PulledMessage<M>>
where
    M: DeserializeOwned,
    T: Fn(
        &RawPulledMessageEnvelope,
        Value,
    ) -> Result<Value, Box<dyn StdError + Send + Sync + 'static>>,
{
    envelopes
        .into_iter()
        .map(|envelope| {
            let message = envelope
                .message
                .data
                .as_ref()
                .ok_or(Error::NoData)
                .and_then(|data| base64::decode(data).map_err(|source| Error::NoBase64 { source }))
                .and_then(|bytes| {
                    serde_json::from_slice::<Value>(&bytes)
                        .map_err(|source| Error::Deserialize { source })
                })
                .and_then(|value| {
                    transform(&envelope, value).map_err(|source| Error::Transform { source })
                })
                .and_then(|value| {
                    serde_json::from_value(value).map_err(|source| Error::Deserialize { source })
                });
            let RawPulledMessageEnvelope {
                ack_id,
                message:
                    RawPulledMessage {
                        data: _,
                        attributes,
                        id,
                        publish_time,
                        ordering_key,
                    },
                delivery_attempt,
            } = envelope;
            PulledMessage {
                ack_id,
                message,
                attributes,
                id,
                publish_time,
                ordering_key,
                delivery_attempt,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{deserialize, RawPulledMessage, RawPulledMessageEnvelope};
    use anyhow::anyhow;
    use serde::Deserialize;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::error::Error as StdError;
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    const TIME: &str = "2022-02-20T22:02:20.123456789Z";

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    enum Message {
        Foo { text: String },
        Bar { text: String },
    }

    #[test]
    fn test_deserialize_ok() {
        let envelopes = vec![
            RawPulledMessageEnvelope {
                ack_id: "ack_id".to_string(),
                message: RawPulledMessage {
                    data: Some(base64::encode(json!({"text": "test"}).to_string())),
                    attributes: Some(HashMap::from([("type".to_string(), "Foo".to_string())])),
                    id: "id".to_string(),
                    publish_time: OffsetDateTime::parse(TIME, &Rfc3339).unwrap(),
                    ordering_key: Some("ordering_key".to_string()),
                },
                delivery_attempt: 1,
            },
            RawPulledMessageEnvelope {
                ack_id: "ack_id".to_string(),
                message: RawPulledMessage {
                    data: Some(base64::encode(json!({"Bar": {"text": "test"}}).to_string())),
                    attributes: Some(HashMap::from([("version".to_string(), "v2".to_string())])),
                    id: "id".to_string(),
                    publish_time: OffsetDateTime::parse(TIME, &Rfc3339).unwrap(),
                    ordering_key: None,
                },
                delivery_attempt: 1,
            },
        ];
        let pulled_messages = deserialize::<Message, _>(envelopes, transform);
        assert_eq!(pulled_messages.len(), 2);

        let pulled_message = &pulled_messages[0];
        assert_eq!(pulled_message.id, "id".to_string());
        assert_eq!(pulled_message.ack_id, "ack_id".to_string());
        assert_eq!(
            pulled_message.attributes,
            Some(HashMap::from([("type".to_string(), "Foo".to_string())]))
        );
        assert!(pulled_message.message.is_ok());
        let message = pulled_message.message.as_ref().unwrap();
        assert_eq!(
            *message,
            Message::Foo {
                text: "test".to_string()
            }
        );

        let pulled_message = &pulled_messages[1];
        assert_eq!(pulled_message.id, "id".to_string());
        assert_eq!(pulled_message.ack_id, "ack_id".to_string());
        assert!(pulled_message.message.is_ok());
        let message = pulled_message.message.as_ref().unwrap();
        assert_eq!(
            *message,
            Message::Bar {
                text: "test".to_string()
            }
        );
    }

    fn transform(
        envelope: &RawPulledMessageEnvelope,
        mut value: Value,
    ) -> Result<Value, Box<dyn StdError + Send + Sync + 'static>> {
        let attributes = &envelope.message.attributes;
        match attributes {
            Some(attributes) => match attributes.get("version").map(|v| &v[..]).unwrap_or("v1") {
                "v1" => {
                    let mut type_keys = attributes
                        .keys()
                        .filter(|key| **key == "type" || key.starts_with("type."))
                        .map(|key| (&key[..], key.split(".").skip(1).collect::<Vec<_>>()))
                        .collect::<Vec<_>>();
                    type_keys.sort_unstable_by(|v1, v2| v2.1.len().cmp(&v1.1.len()));
                    for (type_key, json_path) in type_keys {
                        let sub_value = json_path
                            .iter()
                            .fold(Some(&mut value), |v, k| v.and_then(|v| v.get_mut(k)));
                        if let Some(sub_value) = sub_value {
                            let tpe = attributes.get(type_key).unwrap().to_string();
                            *sub_value = json!({ tpe: sub_value });
                        }
                    }
                    Ok(value)
                }
                "v2" => Ok(value),
                unknown => Err(anyhow!("Unknow version `{unknown}`").into()),
            },
            None => Ok(value),
        }
    }
}
