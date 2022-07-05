use lapin::{
    options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Consumer,
};
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum RabbitError {
    #[error(transparent)]
    LapinError(#[from] lapin::Error),
    #[error("{0}: {1}")]
    ConfigError(&'static str, String),
}

#[derive(Deserialize)]
pub struct HarvestReport {
    #[serde(alias = "startTime")]
    pub start_time: String,
    #[serde(alias = "changedResources")]
    pub changed_resources: Vec<HarvestReportChange>,
}

#[derive(Deserialize)]
pub struct HarvestReportChange {
    #[serde(alias = "fdkId")]
    pub fdk_id: String,
}

fn var(key: &'static str) -> Result<String, RabbitError> {
    std::env::var(key).map_err(|e| RabbitError::ConfigError(key, e.to_string()))
}

fn connection_string() -> Result<String, RabbitError> {
    let user = var("RABBIT_USER")?;
    let pass = var("RABBIT_PASS")?;
    let host = var("RABBIT_HOST")?;
    let port = var("RABBIT_PORT")?;

    Ok(format!("amqp://{}:{}@{}:{}/%2f", user, pass, host, port))
}

pub async fn connect() -> Result<Channel, RabbitError> {
    let options = ConnectionProperties::default()
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);

    let uri = connection_string()?;
    let connection = Connection::connect(&uri, options).await?;
    let channel = connection.create_channel().await?;
    Ok(channel)
}

pub async fn setup(channel: &Channel) -> Result<(), RabbitError> {
    channel
        .queue_declare(
            "fdk-dataset-event-publisher",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_bind(
            "fdk-dataset-event-publisher",
            "harvests",
            "datasets.harvested",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    Ok(())
}

pub async fn create_consumer(channel: &Channel) -> Result<Consumer, RabbitError> {
    let consumer = channel
        .basic_consume(
            "fdk-dataset-event-publisher",
            "fdk-dataset-event-publisher",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    Ok(consumer)
}
