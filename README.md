# Google Cloud Pub/Sub client

[![Crates.io][crates-badge]][crates-url]
[![license][license-badge]][license-url]
[![build][build-badge]][build-url]
[![docs][docs-badge]][docs-url]

[crates-badge]: https://img.shields.io/crates/v/pub-sub-client
[crates-url]: https://crates.io/crates/pub-sub-client
[license-badge]: https://img.shields.io/github/license/hseeberger/pub-sub-client
[license-url]: https://github.com/hseeberger/pub-sub-client/blob/main/LICENSE
[build-badge]: https://img.shields.io/github/actions/workflow/status/hseeberger/pub-sub-client/ci.yaml
[build-url]: https://github.com/hseeberger/pub-sub-client/actions/workflows/ci.yaml
[docs-badge]: https://img.shields.io/docsrs/pub-sub-client/latest
[docs-url]: https://docs.rs/pub-sub-client/latest/pub_sub_client/

A [Google Cloud Pub/Sub](https://cloud.google.com/pubsub) client library for
[Rust](https://www.rust-lang.org/). It supports publishing, pulling and
acknowledging messages, as well as creating topics and subscriptions; other
management tasks (like deleting or listing) are not yet supported.

Messages can be published and pulled as raw payloads or – when the payload is
JSON – serialized from and deserialized into domain messages (structs or enums)
via [Serde](https://serde.rs/). When pulling, the raw JSON can optionally be
transformed before deserialization, which is handy for schema evolution.

## Features

- Publish messages as typed domain values (serialized via Serde) or as raw payloads.
- Pull and acknowledge messages, with access to metadata like message ID, acknowledge ID, attributes, publish time, ordering key and delivery attempt.
- Optionally transform the pulled JSON before deserialization, e.g. for schema evolution.
- Create topics and subscriptions.
- Support for message attributes, ordering keys and per-request timeouts.

## Installation

``` shell
cargo add pub-sub-client
```

## Usage

All operations are provided by `PubSubClient`. The example below publishes a few
typed messages and then pulls and acknowledges them:

``` rust
use pub_sub_client::{Error, PubSubClient};
use serde::{Deserialize, Serialize};
use std::time::Duration;

const TOPIC_ID: &str = "test";
const SUBSCRIPTION_ID: &str = "test";

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    text: String,
}

async fn run() -> Result<(), Error> {
    // Create a client from a service account key file, refreshing access tokens
    // 30s before they expire. Fails if the key file is missing or malformed.
    let pub_sub_client = PubSubClient::new(
        "secrets/service-account.json",
        Duration::from_secs(30),
    )?;

    // Publish some messages, getting back their IDs.
    let messages = ["Hello", "from pub-sub-client"]
        .into_iter()
        .map(|text| Message { text: text.to_string() })
        .collect::<Vec<_>>();
    let message_ids = pub_sub_client
        .publish(TOPIC_ID, messages, None, None)
        .await?;
    println!("published messages with IDs: {}", message_ids.join(", "));

    // Pull at most 42 messages, deserializing each into a `Message`.
    let pulled_messages = pub_sub_client
        .pull::<Message>(SUBSCRIPTION_ID, 42, None)
        .await?;

    for pulled_message in pulled_messages {
        match pulled_message.message {
            Ok(m) => println!("pulled message with text \"{}\"", m.text),
            Err(e) => eprintln!("ERROR: {e}"),
        }
        pub_sub_client
            .acknowledge(SUBSCRIPTION_ID, vec![&pulled_message.ack_id], None)
            .await?;
        println!("acknowledged message with ID {}", pulled_message.id);
    }

    Ok(())
}
```

Notes:

- `PubSubClient::new` takes the path to a service account key file and a token
  refresh buffer, and returns a `Result` (it fails if the key is missing or
  malformed).
- `publish` and `pull` accept an optional ordering key and per-request timeout
  (both `None` above for brevity).
- Each `PulledMessage.message` is itself a `Result`, since decoding or
  deserializing an individual message may fail; acknowledge it via its
  `ack_id`.

### Raw messages and transformations

Messages can also be published and pulled raw via `publish_raw` and `pull_raw`,
giving direct access to the Base64 encoded payload and all metadata. When
pulling, `pull_with_transform` lets you adjust the raw JSON value before it is
deserialized into a domain message, which is useful for schema evolution. See
[`examples/transform.rs`](examples/transform.rs).

### Examples and local testing

The [`examples`](examples) directory contains complete programs:
[`simple`](examples/simple.rs) for typed publishing and pulling and
[`transform`](examples/transform.rs) for transformations. They read the service
account key path from the `SERVICE_ACCOUNT_PATH` environment variable:

``` shell
SERVICE_ACCOUNT_PATH=secrets/service-account.json cargo run --example simple
```

By default the client talks to `https://pubsub.googleapis.com`. Set the
`PUB_SUB_BASE_URL` environment variable to point it at a different endpoint,
such as a local [Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator).

## Contribution policy

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
