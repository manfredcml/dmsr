use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::Hash;

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    // pub sources: Vec<SourceConfig>,
    // pub targets: Vec<TargetConfig>,
    // pub connectors: Vec<ConnectorConfig>,
    // pub kafka: QueueConfig,
}

impl AppConfig {
    // pub fn validate(&self) -> anyhow::Result<()> {
    //     // Check if sources have duplicated names
    //     let source_names = self.sources.iter().map(|x| &x.name).collect();
    //     let unique_sources = self.check_duplicate(source_names, "Source")?;
    //
    //     // Check if targets have duplicated names
    //     let target_names = self.targets.iter().map(|x| &x.name).collect();
    //     let unique_targets = self.check_duplicate(target_names, "Target")?;
    //
    //     // Check if source_connector have duplicated names
    //     let connector_names = self.connectors.iter().map(|x| &x.name).collect();
    //     self.check_duplicate(connector_names, "Connector")?;
    //
    //     // Check if all source_connector have valid source and target names
    //     let non_existing_names: Vec<&String> = self
    //         .connectors
    //         .iter()
    //         .filter_map(|c| {
    //             if !unique_sources.contains(&c.source_name) {
    //                 Some(&c.source_name)
    //             } else if !unique_targets.contains(&c.target_name) {
    //                 Some(&c.target_name)
    //             } else {
    //                 None
    //             }
    //         })
    //         .collect();
    //
    //     if !non_existing_names.is_empty() {
    //         return Err(anyhow::anyhow!(
    //             "Connectors have invalid source or target names - {:?}",
    //             non_existing_names
    //         ));
    //     }
    //
    //     Ok(())
    // }

    fn check_duplicate<T>(&self, vec: Vec<T>, config_name: &str) -> anyhow::Result<HashSet<T>>
    where
        T: Eq + Hash + Clone,
    {
        let mut unique: HashSet<T> = HashSet::new();

        vec.iter().all(|x| unique.insert(x.clone()));

        if unique.len() != vec.len() {
            return Err(anyhow::anyhow!(
                "Duplicated names in config - {:?}",
                config_name
            ));
        }

        Ok(unique)
    }
}
