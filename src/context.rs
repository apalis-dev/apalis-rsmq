use std::{any::type_name, convert::Infallible};

use apalis_core::{task::metadata::MetadataExt, task_fn::FromRequest};
use serde::{de::{DeserializeOwned, Error}, Deserialize, Serialize};
use serde_json::Map;

use crate::RsMqTask;

/// Context for RedisMq messages
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RedisMqContext {
    max_attempts: usize,
    meta: Map<String, serde_json::Value>,
}

impl RedisMqContext {
    /// Creates a new RedisMqContext
    pub fn new(max_attempts: usize) -> Self {
        Self {
            max_attempts,
            meta: Map::new(),
        }
    }

    /// Sets the max attempts
    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the metadata
    pub fn with_meta(mut self, meta: Map<String, serde_json::Value>) -> Self {
        self.meta = meta;
        self
    }

    /// Gets the max attempts
    pub fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    /// Gets the metadata
    pub fn meta(&self) -> &Map<String, serde_json::Value> {
        &self.meta
    }
}

impl<Req: Sync> FromRequest<RsMqTask<Req>> for RedisMqContext {
    type Error = Infallible;
    async fn from_request(req: &RsMqTask<Req>) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}

impl<T: Serialize + DeserializeOwned> MetadataExt<T> for RedisMqContext {
    type Error = serde_json::Error;
    fn inject(&mut self, value: T) -> Result<(), Self::Error> {
        let json_value = serde_json::to_value(value)?;
        self.meta.insert(type_name::<T>().to_owned(), json_value);
        Ok(())
    }
    fn extract(&self) -> Result<T, Self::Error> {
        if let Some(value) = self.meta.get(type_name::<T>()) {
            let deserialized: T = T::deserialize(value)?;
            Ok(deserialized)
        } else {
            Err(serde_json::Error::custom(format!(
                "No metadata found for type: {}",
                type_name::<T>()
            )))
        }
    }
}
