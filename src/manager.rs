use crate::{PubSubClient, error::Error};
use reqwest::Method;
use serde::Serialize;
use serde_json::json;
use std::time::Duration;
use tracing::debug;

#[derive(Debug, Serialize)]
struct CreateSubscriptionRequest {
    topic: String,
}

impl PubSubClient {
    /// Create the topic with the given ID, optionally using the given request `timeout`.
    #[tracing::instrument]
    pub async fn create_topic(
        &self,
        topic_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        let url = self.topic_url(topic_id);
        let response = self
            .send_request(Method::PUT, &url, &json!({}), timeout)
            .await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code(response).await);
        }

        debug!(topic_id, "successfully created topic");
        Ok(())
    }

    /// Create the subscription with the given ID for the topic with the given ID, optionally using
    /// the given request `timeout`.
    #[tracing::instrument]
    pub async fn create_subscription(
        &self,
        subscription_id: &str,
        topic_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        let url = self.subscription_url(subscription_id);
        let request = CreateSubscriptionRequest {
            topic: format!("projects/{}/topics/{topic_id}", self.project_id),
        };
        let response = self
            .send_request(Method::PUT, &url, &request, timeout)
            .await?;

        if !response.status().is_success() {
            return Err(Error::unexpected_http_status_code(response).await);
        }

        debug!(
            subscription_id,
            topic_id, "successfully created subscription"
        );
        Ok(())
    }
}
