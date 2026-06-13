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

Google Cloud Pub/Sub client library in [Rust](https://www.rust-lang.org/). Currently publishing, pulling and acknowledging are supported, but no management tasks like creating topics or subscriptions.

Messages can either be published/pulled as raw or, if the payload is JSON data, serialized from/deserialized into domain messages (structs or enums) via [Serde](https://serde.rs/) and [Serde JSON](https://docs.rs/serde_json). Both raw `RawPulledMessageEnvelope`s and "typed" `PulledMessage`s expose metadata like message ID, acknowledge ID, attributes, etc.

Aside from straightforward deserialization it is also possible to first transform the pulled JSON values before deserializing into domain messages which allows for generally adjusting the JSON structure as well as schema evolution.

## Features

- Publish messages as typed domain values (serialized via Serde) or as raw payloads.
- Pull and acknowledge messages, with access to metadata like message ID, acknowledge ID, attributes, publish time, ordering key and delivery attempt.
- Optionally transform the pulled JSON before deserialization, e.g. for schema evolution.
- Support for message attributes, ordering keys and per-request timeouts.

## Installation

``` shell
cargo add pub-sub-client
```

## Usage

Typically we want to use domain message:

``` rust
#[derive(Debug, Serialize, Deserialize)]
struct Message {
    text: String,
}
```

To publish `Message` we need to derive `Serialize` and to pull it we need to derive `Deserialize`.

First create a `PubSubClient`, giving the path to a service account key file and the duration to refresh access tokens before they expire:

``` rust
let pub_sub_client = PubSubClient::new(
    "secrets/cryptic-hawk-336616-e228f9680cbc.json",
    Duration::from_secs(30),
)?;
```

Things could go wrong, e.g. if the service account key file does not exist or is malformed, hence a `Result` is returned.

Then we call `publish` to publish some `messages` using the given `TOPIC_ID` and – if all is good – get back the message IDs; we do not use an ordering key nor a request timeout here and below for simplicity:

``` rust
let messages = vec!["Hello", "from pub-sub-client"]
    .iter()
    .map(|s| s.to_string())
    .map(|text| Message { text })
    .collect::<Vec<_>>();
let message_ids = pub_sub_client
    .publish(TOPIC_ID, messages, None, None)
    .await?;
let message_ids = message_ids.join(", ");
println!("Published messages with IDs: {message_ids}");
```

Next we call `pull` to get at most the given `42` messages from the given `SUBSCRIPTION_ID`:

``` rust
let pulled_messages = pub_sub_client
    .pull::<Message>(SUBSCRIPTION_ID, 42, None)
    .await?;
```

Of course pulling which happens via HTTP could fail, hence we get back another `Result`.

Finally we handle the pulled messages; for simplicity we only deal with the happy path here, i.e. when the deserialization was successful:

``` rust
for pulled_message in pulled_messages {
    match pulled_message.message {
        Ok(m) => println!("Pulled message with text \"{}\"", m.text),
        Err(e) => eprintln!("ERROR: {e}"),
    }

    pub_sub_client
        .acknowledge(SUBSCRIPTION_ID, vec![&pulled_message.ack_id], None)
        .await?;
    println!("Acknowledged message with ID {}", pulled_message.id);
}
```

For successfully deserialized messages we call `acknowledge` with the acknowledge ID taken from the envelope.

### Raw messages and transformations

The walkthrough above uses typed domain messages, but messages can also be published and pulled raw via `publish_raw` and `pull_raw`, giving direct access to the Base64 encoded payload and all metadata. When pulling, `pull_with_transform` lets you adjust the raw JSON value before it is deserialized into a domain message, which is handy for schema evolution.

See the [`examples`](examples) directory for complete programs: [`simple`](examples/simple.rs) for typed publishing and pulling and [`transform`](examples/transform.rs) for transformations.

## Contribution policy ##

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.

## License ##

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

