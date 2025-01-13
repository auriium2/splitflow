use chrono::{DateTime, Utc};
use futures::stream;
use rand::prelude::SliceRandom;
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


//EDGAR bypass ;)

fn generate_domain_from_name(company_name: &str) -> String {
    company_name
        .to_lowercase()               // Convert to lowercase
        .replace(" ", "")              // Remove spaces
        .replace("'", "")              // Remove apostrophes
        .replace("&", "and")           // Replace & with "and"
}

fn generate_company_name_and_email() -> (String, String) {
    let company_prefixes = vec![
        "Tech", "Global", "Future", "Net", "Data", "Sky", "Bright", "Prime", "Green",
        "Cloud", "Quantum", "Innovative", "Smart", "Blue", "Secure", "NextGen"
    ];
    let company_suffixes = vec![
        "Solutions", "Corp", "Systems", "Holdings", "Networks", "Consulting", "Group",
        "Technologies", "Ventures", "Partners", "Industries", "Services", "Enterprises"
    ];

    let mut rng = rand::thread_rng();

    let prefix = company_prefixes.choose(&mut rng).unwrap();
    let suffix = company_suffixes.choose(&mut rng).unwrap();
    let company_name = format!("{} {}", prefix, suffix);

    let domain_name = generate_domain_from_name(&company_name);

    let email = format!("admin@{}.com", domain_name);

    (company_name, email)
}
