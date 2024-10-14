use std::sync::atomic::AtomicBool;
use serde::{Deserialize, Serialize};

use sled::Tree;

// CONFIG STUFF
const APP_NAME: &str = "strider";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StriderConfig {
    pub discord_token: String,
}
impl Default for StriderConfig {
    fn default() -> Self {
        StriderConfig {
            discord_token: "MTI5NDQ2MDM3NzQ5NjU1NTU3MQ.Gn0pt_.rVieVlz58vTxDUU7gT1AaxKlFVOUnsF_cJBn8g".to_string()
        }
    }
}



pub struct Core {
    pub is_init: AtomicBool,
    pub config: StriderConfig,
    
    /// map signpost key -> signpost location OR role
    pub discord_db: Tree,
    
    /// i have no idea
    pub general_knowledge_db: Tree,
    
    /// map &str -> bool
    pub document_db: Tree,
    
    /// map company -> playdata
    pub play_db: Tree
}


fn assert_send_sync<T: Send + Sync>(t: T) {}

pub fn load_data() -> anyhow::Result<Core> {
    let cfg: StriderConfig = confy::load(APP_NAME, None)?;
    let path = confy::get_configuration_file_path(APP_NAME, "config")?;
    log::info!("The configuration file path is: {:#?}", path);
    
    let path = confy::get_configuration_file_path(APP_NAME, None)?;
    log::info!("The database file path is: {:#?}", &path);
    let db = sled::open("db")?;
    let discord_db = db.open_tree("signpost_db")?; //TODO fix this shit
    let general_knowledge_db = db.open_tree(b"knowledge_db")?;
    let company_db = db.open_tree(b"company_db")?;
    let play_db = db.open_tree(b"play_db")?;


    Ok(Core {
        is_init: AtomicBool::new(false),
        config: cfg,
        
        discord_db,
        general_knowledge_db,
        document_db: company_db,
        play_db,
    })
}
