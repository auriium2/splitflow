use chrono::{DateTime, Utc};
use futures::stream;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;

pub mod scrape;
pub mod feed;

#[repr(C)]
pub struct CompanyKnowledge {
    /*
    The time in which this company has been known to strider. 
    If this company was discovered via RSS feed it will be a system now.
    If this company was discovered via nightly scrape it will 
     */
    knowledge_start: DateTime<Utc>, 
    knowledge_end: DateTime<Utc>
    

}

#[repr(C)]
#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct ArbitragePlay {
    announced: DateTime<Utc>,
    expiring: DateTime<Utc>
}

