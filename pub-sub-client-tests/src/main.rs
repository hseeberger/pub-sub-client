use pub_sub_client::publisher::PublisherMessage;
use pub_sub_client::PublisherMessage;
use serde::Serialize;

#[derive(Debug, Serialize, PublisherMessage)]
struct Message {
    text: String,
}

fn main() {}
