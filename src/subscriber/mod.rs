use crate::{Error, PubSubClient};
use reqwest::Response;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::time::Duration;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct PulledMessage<M: DeserializeOwned> {
    pub ack_id: String,
    pub message: M,
    pub attributes: HashMap<String, String>,
    pub id: String,
    pub publish_time: OffsetDateTime,
    pub ordering_key: Option<String>,
    pub delivery_attempt: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceivedMessage {
    pub ack_id: String,
    #[serde(rename = "message")]
    pub pub_sub_message: PubSubMessage,
    #[serde(default)] // The Pub/Sub emulator does not send this field!
    pub delivery_attempt: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubMessage {
    pub data: String,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
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
    #[serde(default)]
    received_messages: Vec<ReceivedMessage>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AcknowledgeRequest<'a> {
    ack_ids: Vec<&'a str>,
}

impl PubSubClient {
    pub async fn pull<M: DeserializeOwned>(
        &self,
        subscription_id: &str,
        max_messages: u32,
        timeout: Option<Duration>,
    ) -> Result<Vec<Result<PulledMessage<M>, Error>>, Error> {
        self.pull_with_transform(subscription_id, max_messages, timeout, |_, value| Ok(value))
            .await
    }

    pub async fn pull_with_transform<M, T>(
        &self,
        subscription_id: &str,
        max_messages: u32,
        timeout: Option<Duration>,
        transform: T,
    ) -> Result<Vec<Result<PulledMessage<M>, Error>>, Error>
    where
        M: DeserializeOwned,
        T: Fn(&ReceivedMessage, Value) -> Result<Value, Box<dyn StdError + Send + Sync + 'static>>,
    {
        let received_messages = self
            .pull_raw(subscription_id, max_messages, timeout)
            .await?;
        let messages = deserialize(received_messages, transform);
        Ok(messages)
    }

    pub async fn pull_raw(
        &self,
        subscription_id: &str,
        max_messages: u32,
        timeout: Option<Duration>,
    ) -> Result<Vec<ReceivedMessage>, Error> {
        let request = PullRequest { max_messages };
        let response = self
            .send_request(&self.url(subscription_id, "pull"), &request, timeout)
            .await?;

        if !response.status().is_success() {
            return Err(unexpected_http_status_code(response).await);
        }

        let received_messages = response
            .json::<PullResponse>()
            .await
            .map_err(|source| Error::UnexpectedHttpResponse { source })?
            .received_messages;
        Ok(received_messages)
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
            .send_request(&self.url(subscription_id, "acknowledge"), &request, timeout)
            .await?;

        if !response.status().is_success() {
            return Err(unexpected_http_status_code(response).await);
        }

        Ok(())
    }

    fn url(&self, subscription_id: &str, action: &str) -> String {
        let base_url = &self.base_url;
        let project_id = &self.project_id;
        format!("{base_url}/v1/projects/{project_id}/subscriptions/{subscription_id}:{action}")
    }
}

fn deserialize<M, T>(
    received_messages: Vec<ReceivedMessage>,
    transform: T,
) -> Vec<Result<PulledMessage<M>, Error>>
where
    M: DeserializeOwned,
    T: Fn(&ReceivedMessage, Value) -> Result<Value, Box<dyn StdError + Send + Sync + 'static>>,
{
    received_messages
        .into_iter()
        .map(|received_message| {
            base64::decode(&received_message.pub_sub_message.data)
                .map_err(|source| Error::NoBase64 { source })
                .and_then(|decoded_data| {
                    serde_json::from_slice::<Value>(&decoded_data)
                        .map_err(|source| Error::Deserialize { source })
                })
                .and_then(|value| {
                    transform(&received_message, value)
                        .map_err(|source| Error::Transform { source })
                })
                .and_then(|transformed_value| {
                    serde_json::from_value(transformed_value)
                        .map_err(|source| Error::Deserialize { source })
                })
                .map(|message| {
                    let ReceivedMessage {
                        ack_id,
                        pub_sub_message:
                            PubSubMessage {
                                data: _,
                                attributes,
                                id,
                                publish_time,
                                ordering_key,
                            },
                        delivery_attempt,
                    } = received_message;
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
        })
        .collect()
}

async fn unexpected_http_status_code(response: Response) -> Error {
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

#[cfg(test)]
mod tests {
    use super::{deserialize, Error, PubSubMessage, PulledMessage, ReceivedMessage};
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
        let received_messages = vec![
            ReceivedMessage {
                ack_id: "ack_id".to_string(),
                pub_sub_message: PubSubMessage {
                    data: base64::encode(json!({"text": "test"}).to_string()),
                    attributes: HashMap::from([("type".to_string(), "Foo".to_string())]),
                    id: "id".to_string(),
                    publish_time: OffsetDateTime::parse(TIME, &Rfc3339).unwrap(),
                    ordering_key: Some("ordering_key".to_string()),
                },
                delivery_attempt: 1,
            },
            ReceivedMessage {
                ack_id: "ack_id".to_string(),
                pub_sub_message: PubSubMessage {
                    data: base64::encode(json!({"Bar": {"text": "test"}}).to_string()),
                    attributes: HashMap::from([("version".to_string(), "v2".to_string())]),
                    id: "id".to_string(),
                    publish_time: OffsetDateTime::parse(TIME, &Rfc3339).unwrap(),
                    ordering_key: None,
                },
                delivery_attempt: 1,
            },
        ];
        let pulled_messages_result: Vec<Result<PulledMessage<Message>, Error>> =
            deserialize(received_messages, transform);
        assert_eq!(pulled_messages_result.len(), 2);

        assert!(pulled_messages_result[0].is_ok());
        let pulled_message = pulled_messages_result[0].as_ref().unwrap();
        assert_eq!(pulled_message.id, "id".to_string());
        assert_eq!(pulled_message.ack_id, "ack_id".to_string());
        assert_eq!(
            pulled_message.attributes,
            HashMap::from([("type".to_string(), "Foo".to_string())])
        );
        assert_eq!(
            pulled_message.message,
            Message::Foo {
                text: "test".to_string()
            }
        );

        assert!(pulled_messages_result[1].is_ok());
        let pulled_message = pulled_messages_result[1].as_ref().unwrap();
        assert_eq!(pulled_message.id, "id".to_string());
        assert_eq!(pulled_message.ack_id, "ack_id".to_string());
        assert_eq!(
            pulled_message.message,
            Message::Bar {
                text: "test".to_string()
            }
        );
    }

    fn transform(
        received_message: &ReceivedMessage,
        mut value: Value,
    ) -> Result<Value, Box<dyn StdError + Send + Sync + 'static>> {
        let attributes = &received_message.pub_sub_message.attributes;
        match attributes.get("version").map(|v| &v[..]).unwrap_or("v1") {
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
        }
    }
}
