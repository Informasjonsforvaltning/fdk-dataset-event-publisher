use schema_registry_converter::{
    async_impl::schema_registry::{post_schema, SrSettings},
    schema_registry_common::{SchemaType, SuppliedSchema},
};
use serde_derive::Serialize;
use tracing::instrument;

use crate::kafka::KafkaError;

#[derive(Debug, Clone, Serialize)]
pub enum DatasetEventType {
    #[serde(rename = "DATASET_HARVESTED")]
    DatasetHarvested,
}

#[derive(Debug, Serialize)]
pub struct DatasetEvent {
    #[serde(rename = "type")]
    pub event_type: DatasetEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}

#[instrument]
pub async fn setup_schemas(sr_settings: &SrSettings) -> Result<u32, KafkaError> {
    let schema = SuppliedSchema {
        name: Some("no.fdk.dataset.DatasetEvent".to_string()),
        schema_type: SchemaType::Avro,
        schema: r#"{
                "name": "DatasetEvent",
                "namespace": "no.fdk.dataset",
                "type": "record",
                "fields": [
                    {
                        "name": "type",
                        "type": {
                            "type": "enum",
                            "name": "DatasetEventType",
                            "symbols": ["DATASET_HARVESTED"]
                        }
                    },
                    {"name": "fdkId", "type": "string"},
                    {"name": "graph", "type": "string"},
                    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
                ]
            }"#
        .to_string(),
        references: vec![],
    };

    tracing::info!("registering schema");
    let result = post_schema(
        sr_settings,
        "no.fdk.dataset.DatasetEvent".to_string(),
        schema,
    )
    .await?;
    Ok(result.id)
}
