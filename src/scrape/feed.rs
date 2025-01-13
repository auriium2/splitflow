//This should contain an async task which continuously listens for new filings on the 8-k and 6-k RSS feeds
//When it detects new stuff it should do something...

#![feature(async_closure)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use feed_rs::parser;
use futures::{stream, StreamExt};
use reqwest::{Client, header, Proxy};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sled::Batch;
use tokio::{join, sync};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;

use crate::billboard::console::{ConsoleMessage, DateCommand};
use crate::bootstrap::Core;
use crate::scrape::generate_company_name_and_email;

use scraper::{Html, Selector};
use crate::buysell::BuysellCommand;

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
    Failed,
}

const LAST_RSS_KEY: &[u8; 8] = b"last_rss";
const SEC_6K_LINK: &str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=6-K&company=&dateb=&owner=include&start=0&count=100&output=atom";
const SEC_8K_LINK: &str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-K&company=&dateb=&owner=include&start=0&count=100&output=atom";

pub struct RSSTask {
    client: Client,
    core: Arc<Core>,
    rx: Receiver<RSSCommand>,
    tx_console: Sender<DateCommand>,
    tx_orders: Sender<BuysellCommand>
}

impl RSSTask {
    
    pub async fn run(mut self) {
        
    }
    
}

pub async fn task_update_rss(core: Arc<Core>, mut rx: Receiver<RSSCommand>, tx_console: Sender<DateCommand>) -> Result<()> {

    let client = Client::builder()
        //.proxy(Proxy::https("socks5://p.webshare.io:80")?.basic_auth("desouisv-rotate","fw7rphncsa5e"))
        .build()?;
    
    let mut billboard_jail: BillboardState = BillboardState {
        time_stats: TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: 0, found_day: 0, found_week: 0, found_month: 0 },
        doing: RSSGoal::Idle,
        tasks: vec![],
    };

    while let Some(command) = rx.recv().await {
        match command {
            RSSCommand::ResetDay => {
                billboard_jail.time_stats = TimeStats { scanned_day: 0, scanned_week: billboard_jail.time_stats.scanned_week, scanned_month: billboard_jail.time_stats.scanned_month, found_day: 0, found_week: billboard_jail.time_stats.found_week, found_month: billboard_jail.time_stats.found_month }
            }
            RSSCommand::ResetWeek => {
                billboard_jail.time_stats = TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: billboard_jail.time_stats.scanned_month, found_day: 0, found_week: 0, found_month: billboard_jail.time_stats.scanned_month }
            }
            RSSCommand::ResetMonth => {
                billboard_jail.time_stats = TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: 0, found_day: 0, found_week: 0, found_month: 0 }
            }
            RSSCommand::RunProcess => {
                let rs: Result<()> = {
                    info!("[SCAN] Scanning top level");
                    tx_console.send(DateCommand::Print(ConsoleMessage::new("[SCAN] Scanning top level...".to_string()), false)).await?;

                    let mut headers = HeaderMap::new();
                    let (company_name, email) = generate_company_name_and_email();
                    let both = format!("{} {}", company_name, email);
                    
                    headers.insert(header::USER_AGENT, HeaderValue::from_str(&*both)?);
                    headers.insert(header::HOST, HeaderValue::from_str("www.sec.gov")?);
                    let future6k = client.get(SEC_6K_LINK).headers(headers.clone()).send();
                    let future8k = client.get(SEC_8K_LINK).headers(headers.clone()).send();
                    let (response6k, response8k) = join!(future6k, future8k);
                    let (resp6k, resp8k) = (response6k?,response8k?);
                    let (status6k, status8k) = (resp6k.status(), resp8k.status());
                    let (body6k, body8k) = (resp6k.text().await?, resp8k.text().await?);

                    info!("[SCAN] Top level OK, status codes [{}] [{}]", status6k, status8k);
                    tx_console.send(DateCommand::Print(
                        ConsoleMessage::new_children(
                            "[SCAN] Top level OK".to_string(), 
                            vec![
                                format!("Status code 6k: [{}] 8k: [{}]", status6k, status8k).to_string(),
                                "Now reading...".to_string()
                            ]
                        ), false)
                    ).await?;

                    let (feed6k,feed8k) = (parser::parse(body6k.as_bytes())?,parser::parse(body8k.as_bytes())?);
                    let (size6k,size8k) = (feed6k.entries.len(), feed8k.entries.len());
                    // Combine entries from both feeds
                    let mut feed_urls = Vec::new();
                    feed_urls.extend(feed6k.entries);
                    feed_urls.extend(feed8k.entries);
                    //
                    let unseen_uuids = feed_urls.iter().filter_map(|a| {
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

                    info!("[READ] Top level OK");
                    tx_console.send(DateCommand::Print(
                        ConsoleMessage::new_children(
                            "[READ] Top level OK".to_string(),
                            vec![
                                format!("Entry size for 6k: [{}] 8k: [{}]", size6k, size8k).to_string(),
                                format!("Unique entries: [{}]", unseen_uuids.len()).to_string(),
                                "Now scanning uniques...".to_string()
                            ]
                        ), false)
                    ).await?;

                    let mut batch: Batch = Batch::default();
                    for e in &unseen_uuids {
                        batch.insert(e.0, bincode::serialize(&true)?); //write to db
                    }

                    //TODO: Write the batch

                    info!("[SCAN] Scanning uniques");
                    tx_console.send(DateCommand::Print(
                        ConsoleMessage::new(
                            "[SCAN] Scanning uniques...".to_string()
                        ), false)
                    ).await?;

                    let max_progress = unseen_uuids.len() as u32;
                    let hub_counter = Arc::new(AtomicI32::new(0));
                    let hub_counter2 = hub_counter.clone();

                    let (killsig_tx, mut killsig_rx) = sync::oneshot::channel::<()>();
                    let ref_client = &client; // Need this to prevent client from being moved into the first map

                    let unseen_links = unseen_uuids.iter().map(|tuple| { tuple.1.clone() }).collect::<Vec<String>>();
                    
                    let mut status_reports: Vec<String> = vec![];
                    let rot = headers.clone();

                    let hub_bodies = join!(
                        async { loop {
                            match killsig_rx.try_recv() {
                                Ok(()) => {
                                    break;
                                }
                                _ => {
                                    let g = format!("[SCAN] Scanned [{}/{}] uniques...", hub_counter.load(Ordering::Relaxed), max_progress);
                                    info!("{}", g.clone());
                                    status_reports.push(g);
                                    sleep(Duration::from_secs(3)).await;
                                }
                            }
                        }},
                        async {
                            let out = fetch_body_bulk(ref_client, unseen_links, hub_counter.clone(), rot).await;
                            killsig_tx.send(()).expect("oops");
                            return out;
                        }
                    ).1?;

                    info!("[SCAN] Extracted [{}/{}] unique entries...", hub_counter2.load(Ordering::Relaxed), max_progress);
                    tx_console.send(DateCommand::Print(
                        ConsoleMessage::new_children(
                            format!("[SCAN] [{}/{}] uniques OK!", hub_counter.load(Ordering::Relaxed), max_progress).to_string(),
                            status_reports
                        ), false)
                    ).await?;


                    let extracted_elements = extract_8k_links(hub_bodies).await?;
                    let filings_counter = Arc::new(AtomicI32::new(0));
                    let filing_bodies = fetch_body_bulk(&client, extracted_elements, filings_counter, headers.clone()).await?;



                    info!("here is element 1: {}", filing_bodies.first().unwrap());

                    



                    

                    //send to buysell + send to general alerter
                    
                    
                    Ok(())
                };
                
                if rs.is_err() {
                    let v = rs.err().unwrap().to_string();
                    error!("something went wrong: {}",v.clone());
                    
                }
            }
            RSSCommand::Die => {
                break;
            }
        }
    }

    Ok(())
}

async fn fetch_body_bulk(c: &Client, urls: Vec<String>, counter: Arc<AtomicI32>, headers: HeaderMap) -> Result<Vec<String>> {
    let concurrency_limit = 5;
    let delay_between_requests = Duration::from_millis(50);

    let results = stream::iter(urls.into_iter().map(|url| {
        let g_move = counter.clone();
        let h_move = headers.clone();
        async move {
            let response = c.get(url).headers(h_move).send().await?;
            let body = response.text().await?;
            g_move.fetch_add(1, Ordering::Relaxed); //increment progress bar
            sleep(delay_between_requests).await;

            Ok(body)
        }
    }))
        .buffer_unordered(concurrency_limit)
        .collect::<Vec<_>>() // Collect results into a Vec
        .await;

    Ok(results.into_iter().filter_map(|i: anyhow::Result<_>| i.ok()).collect::<Vec<_>>())
}

async fn extract_8k_links(bodies: Vec<String>) -> Result<Vec<String>> {
    let table_selector = Selector::parse("table").unwrap();
    let row_selector = Selector::parse("tr").unwrap();
    let cell_selector = Selector::parse("td").unwrap();
    let link_selector = Selector::parse("a").unwrap();

    let results= stream::iter(bodies)
        .map(|body| {
            let table_selector = table_selector.clone();
            let row_selector = row_selector.clone();
            let cell_selector = cell_selector.clone();
            let link_selector = link_selector.clone();
            async move {
                let result = process_body(
                    body.clone(),
                    &table_selector,
                    &row_selector,
                    &cell_selector,
                    &link_selector,
                )
                    .await;

                result
            }
        })
        .buffer_unordered(10) // Adjust concurrency limit as needed
        .collect::<Vec<_>>()
        .await;

    fn convert_vec(results: Vec<Result<String>>) -> Result<Vec<String>> {
        results.into_iter().collect::<Result<Vec<_>, _>>()
    }
    convert_vec(results)
}




async fn process_body(
    body: String,
    table_selector: &Selector,
    row_selector: &Selector,
    cell_selector: &Selector,
    link_selector: &Selector,
) -> Result<String> {
    let document = Html::parse_document(&body);

    let mut found_link: Option<String> = None;

    for table in document.select(table_selector) {
        for row in table.select(row_selector).skip(1) {
            let mut cells = row.select(cell_selector);
            let _ = cells.next();
            let _ = cells.next();

            let document_cell = cells.next();
            let document_link = document_cell
                .and_then(|cell| cell.select(link_selector).next())
                .and_then(|link| link.value().attr("href"))
                .map(|href| format!("https://www.sec.gov{}", href))
                .unwrap_or_default();

            let description_cell = cells.next();
            let description = description_cell
                .map(|cell| {
                    cell.text()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .trim()
                        .to_string()
                })
                .unwrap_or_default();
            
            let desc_upper = description.to_uppercase();
            
            //TODO make this not so shit
            if desc_upper.contains("8-K") || desc_upper.contains("6-K"){
                found_link = Some(document_link);
                break;
            }
        }

        if found_link.is_some() {
            break;
        }
    }

    if found_link.is_none() {
        return Err(anyhow!("oops, didn't find an internal link! Dumping doc {:?}", body));
    }

    Ok(found_link.unwrap())
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
