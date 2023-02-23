use crate::config::AppConfig;
use anyhow::Result;
use std::fs::File;
use std::io::Read;

pub fn load_config(path: &str) -> Result<AppConfig> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let config: AppConfig = serde_yaml::from_str(&contents)?;
    Ok(config)
}
