//This should contain an async task which continuously listens for new filings on the 8-k and 6-k RSS feeds
//When it detects new stuff it should do something...

#![feature(async_closure)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use feed_rs::parser;
use futures::{stream, StreamExt};
use reqwest::{Client, header};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sled::Batch;
use tokio::{join, sync};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;

use crate::billboard::console::{ConsoleMessage, OrderedConsoleCommand};
use crate::bootstrap::Core;

pub enum RSSCommand {
    ResetDay,
    ResetWeek,
    ResetMonth,
    RunProcess,
    Die,
}

struct BillboardState {
    time_stats: TimeStats,
    doing: RSSGoal,
    tasks: Vec<String>,
}

const TIMESTATS_KEY: &[u8; 9] = b"timestats";

#[derive(Copy, Clone, Serialize, Deserialize, Default)]
struct TimeStats {
    scanned_day: i64,
    scanned_week: i64,
    scanned_month: i64,
    found_day: i64,
    found_week: i64,
    found_month: i64,
}

#[derive(Debug)]
pub enum RSSGoal {
    Idle,
    ScanningTopLevel,
    ReadingTopLevel,
    ScanningUnique,
    ReadingUnique,
}

const LAST_RSS_KEY: &[u8; 8] = b"last_rss";
const SEC_6K_LINK: &str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=6-K&company=&dateb=&owner=include&start=0&count=100&output=atom";
const SEC_8K_LINK: &str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-K&company=&dateb=&owner=include&start=0&count=100&output=atom";


pub async fn task_update_rss(core: Arc<Core>, mut rx: Receiver<RSSCommand>, tx_console: Sender<OrderedConsoleCommand>) -> anyhow::Result<()> {
    let client = Client::new();
    //this is so reskarted
    let mut billboard_jail: BillboardState = (BillboardState {
        time_stats: TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: 0, found_day: 0, found_week: 0, found_month: 0 },
        doing: RSSGoal::Idle,
        tasks: vec![],
    });

    while let Some(command) = rx.recv().await {
        match command {
            RSSCommand::ResetDay => {
                billboard_jail.time_stats = TimeStats { scanned_day: 0, scanned_week: billboard_jail.time_stats.scanned_week, scanned_month: billboard_jail.time_stats.scanned_month, found_day: 0, found_week: billboard_jail.time_stats.found_week, found_month: billboard_jail.time_stats.found_month }
            }
            RSSCommand::ResetWeek => {
                (billboard_jail).time_stats = TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: billboard_jail.time_stats.scanned_month, found_day: 0, found_week: 0, found_month: billboard_jail.time_stats.scanned_month }
            }
            RSSCommand::ResetMonth => {
                (billboard_jail).time_stats = TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: 0, found_day: 0, found_week: 0, found_month: 0 }
            }
            RSSCommand::RunProcess => {
                {
                    billboard_jail.doing = RSSGoal::ScanningTopLevel;
                    push_state(&billboard_jail, false, tx_console.clone()).await?;
                }
                println!("scan Top level");


                let mut headers = HeaderMap::new();
                headers.insert(header::USER_AGENT, HeaderValue::from_str("auriium softworks2 admin2@superyuuki2.com")?);
                headers.insert(header::HOST, HeaderValue::from_str("www.sec.gov")?);
                let future6k = client.get(SEC_6K_LINK).headers(headers.clone()).send();
                let future8k = client.get(SEC_8K_LINK).headers(headers).send();
                let (response6k, response8k) = join!(future6k, future8k);
                let (body6k, body8k) = (response6k?.text().await?, response8k?.text().await?);

                {
                    billboard_jail.doing = RSSGoal::ReadingTopLevel;
                    push_state(&(billboard_jail), false, tx_console.clone()).await?;
                }
                println!("Reading top level");
                
                info!("The body is: {}", body6k);

                let feed6k = parser::parse(body6k.as_bytes())?;
                let feed8k = parser::parse(body8k.as_bytes())?;

                // Combine entries from both feeds
                let mut entries = Vec::new();
                entries.extend(feed6k.entries);
                entries.extend(feed8k.entries);

                {
                    billboard_jail.doing = RSSGoal::ScanningTopLevel;
                    billboard_jail.tasks.push(format!("got {} entries!", entries.len()));
                    push_state(&(billboard_jail), false, tx_console.clone()).await?;
                }

                let unseen_ids = entries.iter().filter_map(|a| {
                    let id_copy = a.id.as_str();
                    let out = core.document_db.get(id_copy);
                    if out.is_err() { return None; }
                    if out.unwrap().is_some() { return None; }
                    let ved: Option<(&str, String)> = {
                        let z = a.links.first()?;
                        let rr = z.href.clone();

                        Some((id_copy, rr))
                    };
                    return ved;
                }).collect::<Vec<(&str, String)>>();

                {
                    billboard_jail.tasks.push(format!("got [{}] unique entries!", unseen_ids.len()));
                    push_state(&billboard_jail, false, tx_console.clone()).await?;
                }
                println!("Got entries");

                let mut batch: Batch = Batch::default();
                for e in &unseen_ids {
                    batch.insert(e.0, bincode::serialize(&true)?); //write to db
                }

                sleep(Duration::from_secs(1)).await; //allow cooldown
                billboard_jail.doing = RSSGoal::ScanningUnique;
                billboard_jail.tasks.clear();
                push_state(&billboard_jail, false, tx_console.clone()).await?;

                println!("Scanning unique");
                
                let max_progress = unseen_ids.len() as u32;
                let cur_process = Arc::new(AtomicI32::new(0));
                let cur_process2 = cur_process.clone();

                //rust giving me a FUCKING HEADICACHIIEHUS

                let (a, mut b) = sync::oneshot::channel::<()>();
                let ref_client = &client; // Need this to prevent client from being moved into the first map

                let gh = unseen_ids.iter().map(|tuple| { tuple.1.clone() }).collect::<Vec<String>>();

                let k = join!(
                    async {
                        loop {
                         match b.try_recv() {
                              Ok(()) => {
                                  println!("Somehow done processing the children");
                                  break;
                               }
                               _ => {
                                    let s = format!("Processed [{}/{}] unique entries...", cur_process.load(Ordering::Relaxed), max_progress);;
                                    if billboard_jail.tasks.len() < 1 {
                                        billboard_jail.tasks.push(s);
                                    } else {
                                        billboard_jail.tasks[0] = s;
                                    }

                                   push_state(&billboard_jail, false, tx_console.clone()).await.unwrap();
                                
                                  sleep(Duration::from_secs(5)).await;
                               }
                             }
                        }
                    },
                    async {
                        fetch_buffered(ref_client, gh, cur_process.clone()).await;
                        a.send(())
                    }
                );


                {
                    billboard_jail.tasks[0] = format!("Processed [{}/{}] unique entries...", cur_process2.load(Ordering::Relaxed), max_progress);
                    push_state(&billboard_jail, false, tx_console.clone()).await?;
                }

                println!("Donezon");


                {
                    billboard_jail.doing = RSSGoal::Idle;
                    billboard_jail.tasks.clear();
                    billboard_jail.tasks.push("done lol".to_string());
                    push_state(&billboard_jail, false, tx_console.clone()).await?;
                }
            }
            RSSCommand::Die => {
                break;
            }
        }
    }

    Ok(())
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

async fn fetch_url(c: &Client, url: &str) -> Result<String, reqwest::Error> {
    let response = reqwest::get(url).await?;
    let body = response.text().await?;
    Ok(body)
}

async fn fetch_buffered(c: &Client, urls: Vec<String>, counter: Arc<AtomicI32>) -> Vec<anyhow::Result<String>> {
    let concurrency_limit = 10;
    let delay_between_requests = Duration::from_millis(100);

    let results = stream::iter(urls.into_iter().map(|url| {
        let g_move = counter.clone();
        async move {
            let result = fetch_url(c, &url).await;
            g_move.fetch_add(1, Ordering::Relaxed); //increment progress bar
            sleep(delay_between_requests).await;
            match result {
                Ok(o) => {
                    Ok(o)
                }
                Err(e) => {
                    Err(anyhow::anyhow!(e))
                }
            }
        }
    }))
        .buffer_unordered(concurrency_limit)
        .collect::<Vec<_>>() // Collect results into a Vec
        .await;

    return results;
}


async fn push_state(state: &BillboardState, alert: bool, tx: Sender<OrderedConsoleCommand>) -> anyhow::Result<()> {
    let v = vec![
        ConsoleMessage::new_ord(format!("Documents scanned: today: [{}] week: [{}] month: [{}]", state.time_stats.scanned_day, state.time_stats.scanned_week, state.time_stats.scanned_month), 2),
        ConsoleMessage::new_children_ord(
            format!("RSS Scanner | State: [{:#?}]", state.doing),
            state.tasks.clone(),
            1,
        ),
    ];

    tx.send(OrderedConsoleCommand::Printall(v, alert)).await?;

    Ok(())
}
