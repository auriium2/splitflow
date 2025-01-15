#![feature(async_closure)]

mod browser;
mod rss_presence;
mod rss_inference;

use rayon::prelude::*;

use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use anyhow::{anyhow, Context, Result};
use feed_rs::parser;
use futures::{stream, StreamExt};
use rand::prelude::SliceRandom;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{header, Client, Proxy, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sled::Batch;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use tokio::{join, sync};

use crate::billboard::console::{Console, ConsoleMessage, DateCommand};
use crate::core::{Body, Core, Link, UUID};

use crate::buysell::BuysellCommand;
use scraper::{Html, Selector};
use crate::scrape::rss_presence::RSSPhaseOneDetector;

//TODO we need to rewrite this to deal with Strings better instead of cloning all over the place
//i love rapid development "practices"

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
    tx_console: Sender<DateCommand>
}

struct UUIDAndLink {
    uuid: UUID,
    link: String,
}

impl RSSTask {
    pub fn new(core: Arc<Core>, rx: Receiver<RSSCommand>, tx_console: Sender<DateCommand>) -> Result<Self> {
        let client = Client::builder()
            .proxy(Proxy::https("socks5://p.webshare.io:80")?.basic_auth(core.config.proxy_user.as_str(), core.config.proxy_pass.as_str()))
            .build()?;

        Ok(Self { client, core, rx, tx_console})
    }
    
    async fn log(&self, msg: &str) -> Result<()> {
        info!("{}", msg);
        self.tx_console.send(DateCommand::Print(
            ConsoleMessage::new(
                msg.to_string()
            ), false)
        ).await?;
        
        Ok(())
    }

    fn reset_day() {

    }

    async fn pull_body(&self) -> Result<(String, String, StatusCode, StatusCode, HeaderMap)> {
        let mut headers = HeaderMap::new();
        let (company_name, email) = generate_company_name_and_email();
        let both = format!("{} {}", company_name, email);

        headers.insert(header::USER_AGENT, HeaderValue::from_str(&*both)?);
        headers.insert(header::HOST, HeaderValue::from_str("www.sec.gov")?);
        let future6k = self.client.get(SEC_6K_LINK).headers(headers.clone()).send();
        let future8k = self.client.get(SEC_8K_LINK).headers(headers.clone()).send();
        let (response6k, response8k) = join!(future6k, future8k);
        let (resp6k, resp8k) = (response6k?,response8k?);
        let (status6k, status8k) = (resp6k.status(), resp8k.status());
        let (body6k, body8k) = (resp6k.text().await?, resp8k.text().await?);
        
        Ok((body6k, body8k, status6k, status8k, headers))
    }

    async fn merge_feeds_and_collect(&self, body6k: String, body8k: String) -> Result<(usize, usize, Vec<UUIDAndLink>)> {
        //read the main feeds
        let (feed6k,feed8k) = (parser::parse(body6k.as_bytes())?, parser::parse(body8k.as_bytes())?);
        let (size6k,size8k) = (feed6k.entries.len(), feed8k.entries.len());
        // Combine entries from both feeds

        let mut feed_urls = Vec::new();
        feed_urls.extend(feed6k.entries);
        feed_urls.extend(feed8k.entries);
        //

        let unseen_uuids = stream::iter(feed_urls).filter_map(|a| async move {
            let id_copy = a.id.clone();
            let id_copy2 = id_copy.clone();
            let out = self.core.db.get_filing_document(&id_copy).await;

            //TODO this is a problem, if the db throws db errors we just swallow them
            if out.is_err() { return None; }
            if out.unwrap().is_some() { return None; }
            let ved: Option<UUIDAndLink> = {
                let z = a.links.first()?;
                let rr = z.href.clone();

                Some(UUIDAndLink {uuid: id_copy2, link: rr})
            };
            return ved;
        }).collect::<Vec<UUIDAndLink>>().await;

        Ok((size6k, size8k, unseen_uuids))
    }
    
    async fn visit_intermediaries(&self, headers: &HeaderMap, unseen_ids: Vec<UUIDAndLink>, counter: Arc<AtomicI32>) -> Result<Vec<(UUID, Body)>> {
        let max_progress = unseen_ids.len() as u32;
        let hub_counter = counter.clone();

        let (killsig_tx, mut killsig_rx) = sync::oneshot::channel::<()>();
        let ref_client = &self.client; // Need this to prevent client from being moved into the first map

        let unseen_links = unseen_ids.iter().map(|tuple| { (tuple.uuid.clone(), tuple.link.clone()) }).collect::<Vec<(UUID,Link)>>();

        let mut status_reports: Vec<String> = vec![];
        let base_headers = headers.clone();

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
                            let out = fetch_body_bulk(ref_client, unseen_links, hub_counter.clone(), headers).await;
                            killsig_tx.send(()).expect("oops");
                            return out;
                        }
                    ).1?;

        info!("[SCAN] Visited [{}/{}] unique entries...", hub_counter.load(Ordering::Relaxed), max_progress);
        self.tx_console.send(DateCommand::Print(
            ConsoleMessage::new_children(
                format!("[SCAN] [{}/{}] uniques visited", hub_counter.load(Ordering::Relaxed), max_progress).to_string(),
                status_reports
            ), false)
        ).await?;

        Ok(hub_bodies)
    }

    async fn logic(&self) -> Result<()> {

        let rs: Result<()> = {
            //acquire 8ks and 10ks
            let (body6k, body8k, headers) = { 
                self.log("[SCAN] Scanning top level").await?;

                let (body6k, body8k, status6k, status8k, headers) = self.pull_body().await?;
                let status = status6k.is_success() && status8k.is_success();

                info!("[SCAN] Top level {}, status codes [{}] [{}]", status, status6k, status8k);
                self.tx_console.send(DateCommand::Print(
                    ConsoleMessage::new_children(
                        format!("[SCAN] Top level {}", status).to_string(),
                        vec![
                            format!("Status code 6k: [{}] 8k: [{}]", status6k, status8k).to_string(),
                            "Now reading...".to_string()
                        ]
                    ), false)
                ).await?;

                (body6k, body8k, headers)
            };

            //Merge feeds
            let unseen_ids = { 
                let (size6k, size8k, unseen_ids) = self.merge_feeds_and_collect(body6k, body8k).await?;

                info!("[READ] Merged feeds");
                self.tx_console.send(DateCommand::Print(
                    ConsoleMessage::new_children(
                        "[READ] Merged feeds".to_string(),
                        vec![
                            format!("Entry size for 6k: [{}] 8k: [{}]", size6k, size8k).to_string(),
                            format!("Unique entries: [{}]", unseen_ids.len()).to_string(),
                            "Now scanning uniques...".to_string()
                        ]
                    ), false)
                ).await?;

                unseen_ids
            };

            //batch code to db
            { 
                

                //TODO: Write the batch
            }
            
            //visit intermediaries and extract final urls
            let intermediary_links: Vec<(UUID, Link)> = {
                info!("[SCAN] Visiting intermediaries");
                self.tx_console.send(DateCommand::Print(
                    ConsoleMessage::new(
                        "[SCAN] Visiting intermediaries...".to_string()
                    ), false)
                ).await?;
                
                let counter = Arc::new(AtomicI32::new(0));
                let intermediary_bodies: Vec<(UUID, Body)> = self.visit_intermediaries(&headers, unseen_ids, counter).await?;

                info!("[SCAN] Extracting intermediaries");
                self.tx_console.send(DateCommand::Print(
                    ConsoleMessage::new(
                        "[SCAN] Extracting intermediaries...".to_string()
                    ), false)
                ).await?;
                
                let intermediary_links: Vec<(UUID, Link)> = extract_8k_links(intermediary_bodies).await?;

                info!("[SCAN] Extracted intermediaries");
                self.tx_console.send(DateCommand::Print(
                    ConsoleMessage::new(
                        "[SCAN] Extracted intermediaries".to_string()
                    ), false)
                ).await?;
                
                intermediary_links
            };

            //parse through all bodies
            {
                let filings_counter = Arc::new(AtomicI32::new(0));
                let filing_bodies = fetch_body_bulk(&self.client, intermediary_links, filings_counter, &headers).await?;
                let everything = extract_has_split(filing_bodies).await?;
                
                info!("[SCAN] Extracted intermediaries");
                
                let v = everything.iter().filter(|a| { a.2 }).count();
                
                info!("[SCAN] potential candidates count: {}", v);
            }
            
            

            









            Ok(())
        };

        rs

        /*if rs.is_err() {
            let v = rs.err().unwrap().to_string();
            error!("something went wrong: {}",v.clone());
        }*/
    }

    pub async fn run(mut self) -> Result<()>  {


        /*      let mut billboard_jail: BillboardState = BillboardState {
                  time_stats: TimeStats { scanned_day: 0, scanned_week: 0, scanned_month: 0, found_day: 0, found_week: 0, found_month: 0 },
                  doing: RSSGoal::Idle,
                  tasks: vec![],
              };
              */
        while let Some(command) = self.rx.recv().await {
            match command {
                RSSCommand::RunProcess => {
                    self.logic().await?;
                }
                _ => {
                    break;
                }
            }
        }

        Ok(())
    }



}

async fn fetch_body_bulk(c: &Client, urls: Vec<(UUID, Link)>, counter: Arc<AtomicI32>, headers: &HeaderMap) -> Result<Vec<(UUID, Body)>> {
    let concurrency_limit = 5;
    let delay_between_requests = Duration::from_millis(50);

    let results = stream::iter(urls.into_iter().map(|target| {
        let g_move = counter.clone();
        let h_move = headers.clone();
        async move {
            let response = c.get(target.1).headers(h_move).send().await?;
            let body = response.text().await?;
            g_move.fetch_add(1, Ordering::Relaxed); //increment progress bar
            sleep(delay_between_requests).await;

            Ok((target.0,body))
        }
    }))
        .buffer_unordered(concurrency_limit) //io concurrency
        .collect::<Vec<_>>() // Collect results into a Vec
        .await;

    Ok(results.into_iter().filter_map(|i: anyhow::Result<_>| {i.ok()}).collect::<Vec<_>>())
}

async fn extract_has_split(bodies: Vec<(UUID, Body)>) -> Result<Vec<(UUID, Body, bool)>> {

    let contents = tokio::task::spawn_blocking(move || {
        let detector = Arc::new(RSSPhaseOneDetector::new());
        
        let extracted = bodies.into_par_iter().map(|body| {
            let ae = detector.detect_rss_potential(&*body.1);
            
            return (body.0, body.1, ae)
        }).collect::<Vec<_>>();
        
        extracted
    }).await?;
    
    Ok(contents)
}

async fn extract_8k_links(bodies: Vec<(UUID,Body)>) -> Result<Vec<(UUID,Link)>> {
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
                    body.1,
                    &table_selector,
                    &row_selector,
                    &cell_selector,
                    &link_selector,
                )
                    .await;

                Ok((body.0,result.unwrap()))
            }
        })
        .buffer_unordered(10) //TODO: this probably should be done on a different thread pool than the io pool
        .collect::<Vec<_>>()
        .await;

    fn convert_vec<T>(results: Vec<Result<T>>) -> Result<Vec<T>> {
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
