use crate::connectors::connector::Connector;
use crate::sources_targets::source_kind::SourceKind;
use crate::sources_targets::target_kind::TargetKind;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConnectorConfig {
    pub name: String,
    #[serde(rename = "source")]
    pub source_name: String,
    #[serde(rename = "target")]
    pub target_name: String,
}

impl ConnectorConfig {
    pub fn get_source_kind(&self, map: &HashMap<String, SourceKind>) -> anyhow::Result<SourceKind> {
        match map.get(&self.source_name) {
            Some(source_kind) => Ok(source_kind.clone()),
            None => Err(anyhow::anyhow!(
                "Source {} not found in source map",
                self.source_name
            )),
        }
    }

    pub fn get_target_kind(&self, map: &HashMap<String, TargetKind>) -> anyhow::Result<TargetKind> {
        match map.get(&self.target_name) {
            Some(target_kind) => Ok(target_kind.clone()),
            None => Err(anyhow::anyhow!(
                "Target {} not found in target map",
                self.target_name
            )),
        }
    }

    // pub fn get_connector(&self) -> anyhow::Result<Box<dyn Connector + Send>> {
    //     match self.kind {
    //         ConnectorKind::Kafka => {
    //             let kafka = Kafka::new(&self)?;
    //             Ok(kafka)
    //         }
    //     }
    // }
}
