use dms::kafka::config::KafkaConfig;
use dms::kafka::kafka_client::Kafka;
use dms::kafka::message::KafkaMessage;
use futures::StreamExt;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn test_kafka_new() {
    let kafka_config = KafkaConfig::new(
        "localhost:9092".to_string(),
        "test-config".to_string(),
        "test-offset".to_string(),
        "test_status".to_string(),
    );
    let kafka = Kafka::new(&kafka_config).await;
    assert!(kafka.is_ok());

    let kafka = kafka.unwrap();
    assert_eq!(kafka.config, kafka_config);
}

#[tokio::test]
async fn test_kafka_create_topic() {
    let kafka_config = KafkaConfig::new(
        "localhost:9092".to_string(),
        "test-config".to_string(),
        "test-offset".to_string(),
        "test_status".to_string(),
    );
    let kafka = Kafka::new(&kafka_config).await.unwrap();
    let topic_name = "test-topic-compact";
    let clean_up_policy = "compact";
    let result = kafka.create_topic(topic_name, clean_up_policy).await;
    assert!(result.is_ok());

    let topic_name = "test-topic-delete";
    let clean_up_policy = "delete";
    let result = kafka.create_topic(topic_name, clean_up_policy).await;
    assert!(result.is_ok());

    let mut config = ClientConfig::new();
    config.set("group.id", Uuid::new_v4().to_string());
    config.set("bootstrap.servers", &kafka.config.bootstrap_servers);
    config.set("auto.offset.reset", "smallest");
    let consumer: BaseConsumer = config.create().unwrap();

    let metadata = consumer
        .fetch_metadata(Some("test-topic-compact"), Duration::from_secs(1))
        .unwrap();
    let metadata = metadata.topics();
    for topic in metadata.iter() {
        assert_eq!(topic.name(), "test-topic-compact");
    }

    let metadata = consumer
        .fetch_metadata(Some("test-topic-delete"), Duration::from_secs(1))
        .unwrap();
    let metadata = metadata.topics();
    for topic in metadata.iter() {
        assert_eq!(topic.name(), "test-topic-delete");
    }
}

#[tokio::test]
async fn test_kafka_produce() {
    let kafka_config = KafkaConfig::new(
        "localhost:9092".to_string(),
        "test-config".to_string(),
        "test-offset".to_string(),
        "test_status".to_string(),
    );
    let kafka = Kafka::new(&kafka_config).await.unwrap();

    let mut message = KafkaMessage::new();
    message.set_topic("test-topic".to_string());
    message.set_key("test-key".to_string());
    message.set_value("test-value".to_string());
    let result = kafka.produce(message).await;
    assert!(result.is_ok());

    let consumer = kafka.get_stream_consumer("test-topic").unwrap();
    let mut stream = consumer.stream();
    while let Some(message) = stream.next().await {
        match message {
            Ok(message) => {
                let key = message.key().unwrap();
                let value = message.payload().unwrap();
                let key = std::str::from_utf8(key).unwrap();
                let value = std::str::from_utf8(value).unwrap();
                assert_eq!(key, "test-key");
                assert_eq!(value, "test-value");
                break;
            }
            Err(e) => {
                panic!("Error: {:?}", e);
            }
        }
    }
}

#[tokio::test]
async fn test_kafka_poll_with_timeout() {
    let kafka_config = KafkaConfig::new(
        "localhost:9092".to_string(),
        "test-config".to_string(),
        "test-offset".to_string(),
        "test_status".to_string(),
    );
    let kafka = Kafka::new(&kafka_config).await.unwrap();

    let mut message = KafkaMessage::new();
    message.set_topic("test-poll".to_string());
    message.set_key("test-key-1".to_string());
    message.set_value("test-value-1".to_string());
    kafka.produce(message).await.unwrap();

    let mut message = KafkaMessage::new();
    message.set_topic("test-poll".to_string());
    message.set_key("test-key-2".to_string());
    message.set_value("test-value-2".to_string());
    kafka.produce(message).await.unwrap();

    let polled = kafka.poll_with_timeout("test-poll", 1).await;
    assert!(polled.is_ok());
    let polled = polled.unwrap();
    assert_eq!(polled.len(), 2);
    assert_eq!(polled[0].key, "test-key-1");
    assert_eq!(polled[0].value, "test-value-1");
    assert_eq!(polled[1].key, "test-key-2");
    assert_eq!(polled[1].value, "test-value-2");
}
