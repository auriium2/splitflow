use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use mongodb::Client;
use mongodb::options::ClientOptions;
use quick_cache::sync::Cache;
use sled::Tree;
use crate::core::database::{FilingDocument, CoreDB, SignpostDocument};

pub mod database;

// CONFIG STUFF
const APP_NAME: &str = "strider";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StriderConfig {
    pub discord_token: String,
    pub mongo_user: String,
    pub mongo_password: String,
    pub proxy_user: String,
    pub proxy_pass: String,
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
        }
    }
}

pub struct Core {
    pub is_init: AtomicBool,
    pub config: StriderConfig,
    pub db: CoreDB
}


pub async fn load_data() -> anyhow::Result<Core> {
    let path = confy::get_configuration_file_path(APP_NAME, "config")?;
    log::info!("The configuration file path is: {:#?}", path);
    let cfg: StriderConfig = confy::load(APP_NAME, None)?;

    let client = Client::with_uri_str(format!("mongodb+srv://admin:{}@cluster0.5ihgc.mongodb.net/",cfg.mongo_password )).await?;
    let signpost_db = client.database("splitflow").collection::<SignpostDocument>("signposts");
    let filing_db = client.database("splitflow").collection::<FilingDocument>("filings");
    let filing_cache: Cache<UUID, Option<FilingDocument>> = Cache::new(300);

    Ok(Core {
        is_init: AtomicBool::new(false),
        config: cfg,
        db: CoreDB::new(
            signpost_db,
            filing_db,
            filing_cache
        ),
    })
}

pub type UUID = String;
pub type Link = String;
pub type Body = String;