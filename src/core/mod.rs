use crate::core::database::{CoreDB, FilingDocument, SignpostDocument};
use chrono::DateTime;
use mongodb::options::ClientOptions;
use mongodb::Client;
use quick_cache::sync::Cache;
use reqwest::Proxy;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::instrument;
use tracing::{error, info, warn};

pub mod database;

// CONFIG STUFF
const APP_NAME: &str = "splitflow";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StriderConfig {
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
            discord_token: "MTI5NDQ2MDM3NzQ5NjU1NTU3MQ.Gn0pt_.rVieVlz58vTxDUU7gT1AaxKlFVOUnsF_cJBn8g".to_string(),
            mongo_user: "admin".to_string(),
            mongo_password: "DHOeETe48VOOg4WN".to_string(),
            proxy_user: "desouisv-rotate".to_string(),
            proxy_pass: "fw7rphncsa5e".to_string(),
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
    let path = confy::get_configuration_file_path(APP_NAME, "config")?;
    info!("The configuration file path is: {:#?}", path);
    let cfg: StriderConfig = confy::load(APP_NAME, None)?;

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

    let client = reqwest::Client::builder()
        .proxy(
            Proxy::https("socks5://p.webshare.io:80")?
                .basic_auth(cfg.proxy_user.as_str(), cfg.proxy_pass.as_str()),
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
