//This should contain an async task which continuously listens for new filings on the 8-k and 6-k RSS feeds
//When it detects new stuff it should do something...

#![feature(async_closure)]

use std::os::macos::raw::stat;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use feed_rs::parser;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, header, Response};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sled::Batch;
use sled::transaction::{ConflictableTransactionError, ConflictableTransactionResult, TransactionResult, UnabortableTransactionError};
use tokio::join;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;
use crate::billboard::console::{ConsoleCommand, ConsoleMessage, OrderedConsoleCommand, OrderMessage};
use crate::bootstrap::Core;

pub enum RSSCommand {
    ResetDay,
    ResetWeek,
    ResetMonth,
    RunProcess,
    Die
}
struct BillboardState {
    time_stats: TimeStats,
    doing: RSSGoal,
    tasks: Vec<String>
}

const TIMESTATS_KEY: &[u8; 9] = b"timestats";
#[derive(Copy, Clone, Serialize, Deserialize)]
struct TimeStats {
    scanned_day: i64,
    scanned_week: i64,
    scanned_month: i64,
    found_day: i64,
    found_week: i64,
    found_month: i64
}
#[derive(Debug)]
pub enum RSSGoal {
    Idle,
    Scanning
}

const LAST_RSS_KEY: &[u8; 8] = b"last_rss";
const SEC_6K_LINK: &str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=6-K&company=&dateb=&owner=include&start=0&count=100&output=atom";
const SEC_8K_LINK: &str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-K&company=&dateb=&owner=include&start=0&count=100&output=atom";



pub async fn task_update_rss(core: Arc<Core>, mut rx: Receiver<RSSCommand>, tx_console: Sender<OrderedConsoleCommand>) -> anyhow::Result<()> {
    let client = Client::new();
    let mut stats = TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: 0, found_day: 0, found_week: 0, found_month: 0 };
    
    core.general_knowledge_db.get(TIMESTATS_KEY).un
    
    while let Some(command) = rx.recv().await {
        match command {
            RSSCommand::ResetDay => {
                stats = TimeStats { scanned_day: 0, scanned_week: stats.scanned_week, scanned_month: stats.scanned_month, found_day: 0, found_week: stats.found_week, found_month: stats.found_month }
            }
            RSSCommand::ResetWeek => {
                stats = TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: stats.scanned_month, found_day: 0, found_week: 0, found_month: stats.scanned_month }
            }
            RSSCommand::ResetMonth => {
                stats = TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: 0, found_day: 0, found_week: 0, found_month: 0   }
            }
            RSSCommand::RunProcess => {
                
            }
            RSSCommand::Die => {
                
                break;
            }
        }
        let mut headers = HeaderMap::new();
        headers.insert(header::USER_AGENT, HeaderValue::from_str("auriium softworks admin@superyuuki.com")?);
        headers.insert(header::HOST, HeaderValue::from_str("www.sec.gov")?);
        let future6k = client.get(SEC_6K_LINK).headers(headers.clone()).send();
        let future8k = client.get(SEC_8K_LINK).headers(headers).send();
        let (response6k,response8k) = join!(future6k, future8k);
        let (body6k, body8k) = (response6k?.text().await?,response8k?.text().await?);

        let feed6k = parser::parse(body6k.as_bytes())?;
        let feed8k = parser::parse(body8k.as_bytes())?;

        
        
        // Combine entries from both feeds
        let mut entries = Vec::new();
        entries.extend(feed6k.entries);
        entries.extend(feed8k.entries);

        let unseen_ids = entries.iter().filter_map(|a| {
            let id_copy = a.id.as_str();
            let out = core.document_db.get(id_copy);
            if let Ok(opt) = out {
                if opt.is_none() { 
                    
                    
                    return Some(id_copy) 
                } else {None}
            } else { None }
        }).collect::<Vec<&str>>();

        let mut batch: Batch = Batch::default();

        for e in unseen_ids {
            batch.insert(e,bincode::serialize(&true)?); //write to db
        }

        let links = vec![ // A vec of strings representing links
                          "example.net/a".to_owned(),
                          "example.net/b".to_owned(),
                          "example.net/c".to_owned(),
                          "example.net/d".to_owned(),
        ];

        let ref_client = &client; // Need this to prevent client from being moved into the first map
        
        futures::stream::iter(links)
            .map(async move |link: String| {
                let res = ref_client.get(&link).send().await;

                // res.map(|res| res.text().await.unwrap().to_vec())
                match res { // This is where I would usually use `map`, but not sure how to await for a future inside a result
                    Ok(res) => Ok(res.text().await.unwrap()),
                    Err(err) => Err(err),
                }
            })
            .buffer_unordered(10) // Number of connection at the same time
            .filter_map(|c| future::ready(c.ok())) // Throw errors out, do your own error handling here
            .filter_map(|item| {
                if item.contains("abc") {
                    future::ready(Some(item))
                } else {
                    future::ready(None)
                }
            })
            .map(async move |sec_link| {
                let res = ref_client.get(&sec_link).send().await;
                match res {
                    Ok(res) => Ok(res.text().await.unwrap()),
                    Err(err) => Err(err),
                }
            })
            .buffer_unordered(10) // Number of connections for the secondary requests (so max 20 connections concurrently)
            .filter_map(|c| future::ready(c.ok()))
            .for_each(|item| {
                println!("File received: {}", item);
                future::ready(())
            })
            .await;
        
        /*
        let latest_entry_date = entries.iter()
            .filter_map(|e| e.published)
            .max()
            .unwrap_or(Utc::now());*/
/*
        core.general_knowledge_db.insert(
            LAST_RSS_KEY,
            bincode::serialize(&latest_entry_date)?,
        )?;


*/



    }

    Ok(())
}

 

fn push_state(state: &mut BillboardState, tx: &Sender<OrderedConsoleCommand>) {
    let v = vec![
        ConsoleMessage::new_ord(format!("Documents scanned this day: {} week: {} month: {}", state.time_stats.scanned_day, state.time_stats.scanned_week, state.time_stats.scanned_month), 0),
        ConsoleMessage::new_full(
            format!("RSS Scanner v1.0.0 State [ {:#?} ]", state.doing).as_str(),
            tasks,
            1
        )
    ]
}

//lol lmao
async fn query_chatgpt(document_text: &str, client: &Client) -> anyhow::Result<String> {
    let api_key = std::env::var("OPENAI_API_KEY")?;

    let request_body = json!({
        "model": "gpt-4o-mini",
        "messages": [
            { "role": "system", "content": "You are an expert financial analyst." },
            { "role": "user", "content": format!(
                "Please read the following document and provide an analysis on whether the company plans to round up fractional shares in a reverse stock split. Then, classify the plan using one of the following categories: ROUND_UP, ROUND_DOWN, CASH, NOT_SPLIT, OTHER. \n\nOutput the analysis and classification in JSON format.\n\nDocument:\n{}", document_text)
            }
        ]
    });

    let response = client
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&request_body)
        .send()
        .await?;

    let response_json: serde_json::Value = response.json().await?;
    let chatgpt_response = response_json["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or("")
        .to_string();

    Ok(chatgpt_response)
}