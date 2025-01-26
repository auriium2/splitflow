use crate::core::database::{CoreDB, FilingDocument, SignpostDocument};
use mongodb::Client;
use quick_cache::sync::Cache;
use reqwest::Proxy;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;
use tracing::info;

pub mod database;

// CONFIG STUFF
const APP_NAME: &str = "splitflow";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StriderConfig {
    pub redis_url: String,
    pub announcement_channel: String,
    pub discord_token: String,
    pub mongo_user: String,
    pub mongo_password: String,
    pub proxy_user: String,
    pub proxy_pass: String,
    pub gpt_key: String,
}

impl Default for StriderConfig {
    fn default() -> Self {
        //TODO: remove these before we publish to the funny quantjob portfolio
        StriderConfig {
            redis_url: "redis://redis:6379".to_string(),
            announcement_channel: "1332886621804036107".to_string(),
            discord_token: "MTI5NDQ2MDM3NzQ5NjU1NTU3MQ.Gn0pt_.rVieVlz58vTxDUU7gT1AaxKlFVOUnsF_cJBn8g".to_string(),
            mongo_user: "admin".to_string(),
            mongo_password: "DHOeETe48VOOg4WN".to_string(),
            proxy_user: "whokwhmg-rotate".to_string(),
            proxy_pass: "jmditeewz262".to_string(),
            gpt_key: "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".to_string(),
        }
    }
}

pub enum ToastCommand {
    SendToast(String),
}

pub struct Core {
    pub cfg: StriderConfig,
    pub client: reqwest::Client,
    pub db: CoreDB,
}

#[instrument]
pub async fn load_data() -> anyhow::Result<Core> {
    let path = std::path::Path::new("config/splitflow_config.toml");
    info!("The configuration file path is: {:#?}", path);
    let cfg: StriderConfig = confy::load_path(path)?;

    let client = Client::with_uri_str(format!(
        "mongodb+srv://admin:{}@cluster0.5ihgc.mongodb.net/",
        cfg.mongo_password
    ))
    .await?;
    let signpost_db = client
        .database("splitflow")
        .collection::<SignpostDocument>("signposts");
    let filing_db = client
        .database("splitflow")
        .collection::<FilingDocument>("filings");
    let filing_cache: Cache<UUID, Option<Arc<FilingDocument>>> = Cache::new(300);
    info!("Connected to database!");

    let proxy_url = "http://whokwhmg-rotate:jmditeewz262@p.webshare.io:80/";
    let proxy = Proxy::all(proxy_url)?;
    
    let client = reqwest::Client::builder()
        .proxy(
            proxy
            /*Proxy::all("socks5://p.webshare.io:80")?
                .basic_auth(cfg.proxy_user.as_str(), cfg.proxy_pass.as_str()),*/
        )
        .build()?;

    

    Ok(Core {
        cfg,
        client,
        db: CoreDB::new(signpost_db, filing_db, filing_cache),
    })
}

pub type UUID = String;
pub type Link = String;
pub type Body = String;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use quick_cache::sync::Cache;
    use mongodb::Client;
    use reqwest::Client as ReqwestClient;
    use tracing::info;

    #[tokio::test]
    async fn test_load_data() -> anyhow::Result<()> {
        let proxy_url = "http://whokwhmg-rotate:jmditeewz262@p.webshare.io:80/";

        let client = reqwest::Client::builder()
            .proxy(Proxy::all(proxy_url)?)
            .gzip(true)  // Enables automatic decompression
            .build()?;

        let response = client
            .get("https://ipv4.webshare.io/")
            .send()
            .await?;

        // Print the response body
        println!("Response: {}", response.text().await?);

        Ok(())
    }
}