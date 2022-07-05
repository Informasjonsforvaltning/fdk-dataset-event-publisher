use std::env;

use chrono::DateTime;
use error::Error;
use kafka::create_sr_settings;
use lapin::{
    message::{Delivery, DeliveryResult},
    options::BasicAckOptions,
};
use lazy_static::lazy_static;
use rabbit::HarvestReport;
use rdkafka::producer::FutureProducer;
use reqwest::StatusCode;
use schema_registry_converter::async_impl::{avro::AvroEncoder, schema_registry::SrSettings};
use schemas::setup_schemas;

use crate::{kafka::send_event, schemas::DatasetEvent};

mod error;
mod kafka;
mod rabbit;
mod schemas;

lazy_static! {
    pub static ref HARVESTER_API_URL: String =
        env::var("HARVESTER_API_URL").unwrap_or("http://localhost:8080".to_string());
    pub static ref PRODUCER: FutureProducer = kafka::create_producer().unwrap_or_else(|e| {
        tracing::error!(
            error = e.to_string().as_str(),
            "Kafka producer creation error"
        );
        std::process::exit(1);
    });
    pub static ref CLIENT: reqwest::Client =
        reqwest::ClientBuilder::new().build().unwrap_or_else(|e| {
            tracing::error!(
                error = e.to_string().as_str(),
                "reqwest client creation error"
            );
            std::process::exit(1);
        });
    pub static ref SR_SETTINGS: SrSettings = create_sr_settings().unwrap_or_else(|e| {
        tracing::error!(error = e.to_string().as_str(), "SrSettings creation error");
        std::process::exit(1);
    });
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_current_span(false)
        .init();

    setup_schemas(&SR_SETTINGS).await.unwrap_or_else(|e| {
        tracing::error!(error = e.to_string().as_str(), "Schema registration error");
        std::process::exit(1);
    });

    let channel = rabbit::connect().await.unwrap_or_else(|e| {
        tracing::error!(error = e.to_string().as_str(), "Rabbit connection error");
        std::process::exit(1);
    });
    rabbit::setup(&channel).await.unwrap_or_else(|e| {
        tracing::error!(error = e.to_string().as_str(), "Rabbit setup error");
        std::process::exit(1);
    });
    let consumer = rabbit::create_consumer(&channel).await.unwrap_or_else(|e| {
        tracing::error!(
            error = e.to_string().as_str(),
            "Rabbit consumer creation error"
        );
        std::process::exit(1);
    });

    consumer.set_delegate(move |delivery: DeliveryResult| async {
        let delivery = match delivery {
            Ok(Some(delivery)) => delivery,
            Ok(None) => return,
            Err(error) => {
                tracing::error!(
                    error = error.to_string().as_str(),
                    "Failed to consume message"
                );
                return;
            }
        };

        match handle_message(&PRODUCER, &CLIENT, SR_SETTINGS.clone(), &delivery).await {
            Ok(_) => tracing::info!("Successfully processed message"),
            Err(e) => tracing::error!(
                error = e.to_string().as_str(),
                "Failed when processing message"
            ),
        };

        delivery
            .ack(BasicAckOptions::default())
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = e.to_string().as_str(), "Failed to ack message")
            });
    });

    tokio::time::sleep(tokio::time::Duration::MAX).await;
}

async fn handle_message(
    producer: &FutureProducer,
    client: &reqwest::Client,
    sr_settings: SrSettings,
    delivery: &Delivery,
) -> Result<(), Error> {
    let report: Vec<HarvestReport> = serde_json::from_slice(&delivery.data)?;
    let mut encoder = AvroEncoder::new(sr_settings);

    for element in report {
        let timestamp =
            DateTime::parse_from_str(&element.start_time, "%Y-%m-%d %H:%M:%S %z")?.timestamp() * 1000;

        for resource in element.changed_resources {
            tracing::info!(id = resource.fdk_id.as_str(), "Processing dataset");
            if let Some(graph) = get_graph(&client, &resource.fdk_id).await? {
                let message = DatasetEvent {
                    event_type: schemas::DatasetEventType::DatasetHarvested,
                    fdk_id: resource.fdk_id,
                    graph,
                    timestamp,
                };

                send_event(&mut encoder, &producer, message).await?;
            } else {
                tracing::error!(
                    id = resource.fdk_id.as_str(),
                    "Graph not found in harvester"
                );
            }
        }
    }

    Ok(())
}

async fn get_graph(client: &reqwest::Client, id: &String) -> Result<Option<String>, Error> {
    let response = client
        .get(format!("{}/datasets/{}", HARVESTER_API_URL.clone(), id))
        .send()
        .await?;

    match response.status() {
        StatusCode::NOT_FOUND => Ok(None),
        StatusCode::OK => Ok(Some(response.text().await?)),
        _ => Err(format!(
            "Invalid response from harvester: {} - {}",
            response.status(),
            response.text().await?
        )
        .into()),
    }
}
