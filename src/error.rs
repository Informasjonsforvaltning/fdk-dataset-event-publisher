#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    KafkaError(#[from] crate::kafka::KafkaError),
    #[error(transparent)]
    RabbitError(#[from] crate::rabbit::RabbitError),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
    #[error(transparent)]
    ChronoParseError(#[from] chrono::ParseError),
    #[error("{0}")]
    String(String),
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Self::String(e.to_string())
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::String(e)
    }
}
