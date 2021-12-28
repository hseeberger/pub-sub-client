use crate::{Error, ReceivedMessage};
use serde_json::{json, Value};

pub fn identity(_: &ReceivedMessage, value: Value) -> Result<Value, Error> {
    Ok(value)
}

pub fn insert_attribute(
    key: &str,
    received_message: &ReceivedMessage,
    value: Value,
) -> Result<Value, Error> {
    match received_message.message.attributes.get(key) {
        Some(v) => match value {
            Value::Object(mut map) => {
                map.insert(key.to_string(), json!(v));
                Ok(Value::Object(map))
            }
            other => Err(Error::Transform {
                reason: format!("Unexpected JSON value `{}`", other),
            }),
        },
        None => Err(Error::Transform {
            reason: format!("Missing attribute `{}`", key),
        }),
    }
}
