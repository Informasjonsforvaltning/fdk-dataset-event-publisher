use lazy_static::lazy_static;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use schema_registry_converter::{
    async_impl::{
        easy_avro::EasyAvroEncoder, 
        schema_registry::SrSettings},
    schema_registry_common::SubjectNameStrategy,
};
use std::{env, time::Duration};

use crate::schemas::DatasetEvent;

lazy_static! {
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref OUTPUT_TOPIC: String =
        env::var("OUTPUT_TOPIC").unwrap_or("dataset-events".to_string());
    pub static ref SR_SETTINGS: SrSettings = {
        let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");
        let mut sr_settings_builder =
            SrSettings::new_builder(schema_registry_urls.next().unwrap().to_string());
        schema_registry_urls.for_each(|url| {
            sr_settings_builder.add_url(url.to_string());
        });

        let sr_settings = sr_settings_builder
            .set_timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|e| {
                tracing::error!(error = e.to_string().as_str(), "SrSettings creation error");
                std::process::exit(1)
            });

        sr_settings
    };
    // Its much like AvroEncoder but includes a mutex
    static ref AVRO_ENCODER: EasyAvroEncoder = EasyAvroEncoder::new(SR_SETTINGS.clone());
}

#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error(transparent)]
    SRCError(#[from] schema_registry_converter::error::SRCError),
    #[error(transparent)]
    RdkafkaError(#[from] rdkafka::error::KafkaError),
}

pub fn create_producer() -> Result<FutureProducer, KafkaError> {
    let producer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS.clone())
        .set("message.timeout.ms", "5000")
        .create()?;
    Ok(producer)
}

pub async fn send_event(
    producer: &FutureProducer,
    event: DatasetEvent,
) -> Result<(), KafkaError> {
    let key = event.fdk_id.clone();
    let encoded = AVRO_ENCODER
        .encode_struct(
            event,
            &SubjectNameStrategy::RecordNameStrategy("no.fdk.dataset.DatasetEvent".to_string()),
        )
        .await?;

    let record: FutureRecord<String, Vec<u8>> =
        FutureRecord::to(&OUTPUT_TOPIC).key(&key).payload(&encoded);
    producer
        .send(record, Duration::from_secs(0))
        .await
        .map_err(|e| e.0)?;

    Ok(())
}
