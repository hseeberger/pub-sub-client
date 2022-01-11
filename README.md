# Pub-Sub-Client #

Rust library to access Google Cloud Pub/Sub. Currently only pulling from a subscription as well as acknowledging pulled messages is supported.

Messages can either be pulled as raw or, if the payload is JSON data, deserialized into domain messages (structs or enums) via [Serde](https://serde.rs/) and [Serde JSON](https://docs.serde.rs/serde_json). Both raw `ReceivedMessages` and `MessageEnvelopes` holding deserialized messages, expose metadata like message ID, acknowledge ID, attributes, etc.

Aside from straight forward deserialization it is also possible to first transform the pulled JSON values before deserizlizing into domain messages which allows for generally adjusting the JSON structure as well as schema evolution.

## Usage

Typically we want to deserialize into a domain message:

``` rust
#[derive(Debug, Deserialize)]
struct Message {
    text: String,
}
```

First create a `PubSubClient`, giving the path to a service account key file and the duration to refresh access tokens before they expire:

``` rust
let pub_sub_client = PubSubClient::new(
    "secrets/cryptic-hawk-336616-e228f9680cbc.json",
    Duration::from_secs(30),
)?;
```

Things could go wrong, e.g. if the service account key file does not exist or is malformed, hence a `Result` is returned.

Next we call `pull` to get at most the given `42` messages from the given `SUBSCRIPTION`; we do not use a request timeout here for simplicity:

``` rust
let envelopes = pub_sub_client
    .pull::<Message>(SUBSCRIPTION, 42, None)
    .await?;
```

Of course pulling which happens via HTTP could fail, hence we get back another `Result`.

Finally we handle the pulled messages; for simplicity we only deal with the happy path here, i.e. when the deserialization was successful:

``` rust
for envelope in envelopes {
    let envelope = envelope?;
    println!("Message text: {}", envelope.message.text);

    pub_sub_client
        .acknowledge(SUBSCRIPTION, vec![&envelope.ack_id], None)
        .await?;
    println!("Successfully acknowledged");
}
```

For successfully deserialized messages we call `acknowledge` with the acknowledge ID taken from the envelope.

## Contribution policy ##

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.

## License ##

This code is open source software licensed under the
[Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
