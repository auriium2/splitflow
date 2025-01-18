#![feature(async_closure)]

mod browser;
pub mod rss_presence;
pub mod rss_inference;

use rayon::prelude::*;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use feed_rs::parser;
use futures::{stream, StreamExt};
use poise::CreateReply;
use rand::prelude::SliceRandom;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{header, Client, Proxy, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use tokio::{join, sync};
use tracing::{error, info, info_span, instrument, span, warn, Level};

use crate::billboard::console::{Console, ConsoleMessage, DateCommand};
use crate::core::{Body, Core, Link, UUID};

use crate::buysell::BuysellCommand;
use crate::core::database::FilingDocument;
use crate::scrape::rss_inference::{Classification, Inference, LLMInference};
use crate::scrape::rss_presence::{RSSPhaseOneDetector, RssPresence};
use scraper::{Html, Node, Selector};
use serenity::all::{ChannelId, Colour, CreateEmbed, CreateEmbedFooter, CreateMessage, Timestamp};
use tracing::log::trace;

pub enum RSSCommand {
    ResetDay,
    ResetWeek,
    ResetMonth,
    RunProcess,
    Die,
}

struct BillboardState {
    doing: RSSGoal,
    tasks: Vec<String>,
}

const TIMESTATS_KEY: &[u8; 9] = b"timestats";


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
    tx: Sender<BuysellCommand>
}

struct UUIDAndLink {
    uuid: UUID,
    link: String,
}

impl RSSTask {
    pub fn new(core: Arc<Core>, rx: Receiver<RSSCommand>, tx: Sender<BuysellCommand>) -> Result<Self> {
        let client = Client::builder()
            .proxy(Proxy::https("socks5://p.webshare.io:80")?.basic_auth(core.config.proxy_user.as_str(), core.config.proxy_pass.as_str()))
            .build()?;

        Ok(Self { client, core, rx, tx})
    }
    
    #[instrument(skip(self))]
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


    #[instrument(skip(self, body6k, body8k))]
    async fn merge_feeds_and_collect(&self, body6k: String, body8k: String) -> Result<(usize, usize, Vec<UUIDAndLink>)> {

        //read the main feeds
        let (feed6k,feed8k) = (parser::parse(body6k.as_bytes())?, parser::parse(body8k.as_bytes())?);
        let (size6k,size8k) = (feed6k.entries.len(), feed8k.entries.len());
        // Combine entries from both feeds

        let mut feed_urls = Vec::new();
        feed_urls.extend(feed6k.entries);
        feed_urls.extend(feed8k.entries);

        let unseen_uuids = stream::iter(feed_urls).filter_map(|mut individual_entry| async move {
            let id_copy = individual_entry.id;
            let out = self.core.db.get_filing_document(&id_copy).await;

            //TODO this is a problem, if the db throws db errors we just swallow them
            if out.is_err() { return None; }
            if out.unwrap().is_some() { return None; }
            let ved: Option<UUIDAndLink> = {
                let z = individual_entry.links.remove(0);
                let rr = z.href;

                Some(UUIDAndLink {uuid: id_copy, link: rr})
            };
            return ved;
        }).collect::<Vec<UUIDAndLink>>().await;

        Ok((size6k, size8k, unseen_uuids))
    }

    #[instrument(skip(self, headers, unseen_ids, counter))]
    async fn visit_intermediaries(&self, headers: &HeaderMap, unseen_ids: Vec<UUIDAndLink>, counter: Arc<AtomicI32>) -> Result<Vec<(UUID, Body)>> {
        let max_progress = unseen_ids.len() as u32;
        let hub_counter = counter.clone();

        let (killsig_tx, mut killsig_rx) = sync::oneshot::channel::<()>();
        let ref_client = &self.client; // Need this to prevent client from being moved into the first map

        let unseen_links = unseen_ids.into_iter().map(|tuple| { (tuple.uuid, tuple.link) }).collect::<Vec<(UUID,Link)>>();

        let mut status_reports: Vec<String> = vec![];
        let base_headers = headers.clone();

        let hub_bodies = join!(
                        async { loop {
                            match killsig_rx.try_recv() {
                                Ok(()) => {
                                    break;
                                }
                                _ => {
                                    let g = format!("scanned [{}/{}] uniques...", hub_counter.load(Ordering::Relaxed), max_progress);
                                    info!("{}", &g);
                                    status_reports.push(g);
                                    sleep(Duration::from_secs(3)).await;
                                }
                            }
                        }},
                        async {
                            let out = fetch_body_bulk(ref_client, unseen_links, hub_counter.clone(), base_headers).await;
                            killsig_tx.send(()).expect("oops");
                            return out;
                        }
                    ).1?;

        info!("visited [{}/{}] unique entries...", hub_counter.load(Ordering::Relaxed), max_progress);
        Ok(hub_bodies)
    }
    
    #[instrument(skip(self))]
    async fn scan(&self) -> Result<()> {
        let (body6k, body8k, status6k, status8k, headers) = self.pull_body().await?;
        let status = status6k.is_success() && status8k.is_success();
        if !status {
            error!("failed to pull edgar, status codes [{}] [{}]", status6k, status8k);
        }

        //Merge feeds
        let (size6k, size8k, unseen_ids) = self.merge_feeds_and_collect(body6k, body8k).await?;
        info!("found {} unique documents in edgar, status codes [{}] [{}]", unseen_ids.len(), status6k, status8k);

        if unseen_ids.len() == 0 {
            info!("no new entries found, back to sleep");
            
            return Ok(());
        }


        info!("visiting edgar intermediary pages");
        let counter = Arc::new(AtomicI32::new(0));
        let intermediary_bodies: Vec<(UUID, Body)> = self.visit_intermediaries(&headers, unseen_ids, counter).await?;

        info!("extracting 8-k links from intermediaries:");
        let intermediary_links: Vec<(UUID, Link)> = extract_8k_links(intermediary_bodies).await?;

        info!("visiting 8-k pages");
        let filings_counter = Arc::new(AtomicI32::new(0));
        let filing_bodies = fetch_body_bulk(&self.client, intermediary_links, filings_counter, headers).await?;
        let everything = extract_has_split(filing_bodies).await?;

        trace!("scanning 8-k pages for split status");
        let all_filings = everything.into_iter().map(|tuple| {
            let (uuid, body, presence) = tuple;
            let now = Utc::now();
            let filing_document = FilingDocument::new(uuid, now.into(), presence, None, body);

            filing_document
        }).collect::<Vec<FilingDocument>>();
        
        info!("collecting interesting documents");
        let mut key_documents = all_filings.iter()
            .filter(|d| d.is_split.0)
            .map(|d| d.clone()) //todo stop being lazy
            .collect::<Vec<FilingDocument>>();

        info!("pushing filings to db");
        self.core.db.push_filing_documents(all_filings).await?;

        info!("candidate count: {}", key_documents.len());
        let mut split = 0;

        info!("running inference");
        let inference = LLMInference::new(&self.client, &self.core.config.gpt_key);
        
        for document in key_documents.iter_mut() {
            let inference_data = inference.infer(&document.body_contents).await?;
            if inference_data.classification == Classification::RoundUp {
                split += 1;
            }

            //update doc
            document.post_inference = Some(inference_data);
        }
        
        info!("pushing inferred filings to db");
        self.core.db.push_filing_documents(key_documents).await?;
        
        info!("split count: {}", split);
        

        Ok(())
    }

    pub async fn run(mut self) -> Result<()>  {
        while let Some(command) = self.rx.recv().await {
            match command {
                RSSCommand::RunProcess => {
                    self.scan().await?;
                }
                _ => {
                    break;
                }
            }
        }

        Ok(())
    }



}

#[instrument(skip_all)]
async fn fetch_body_bulk(c: &Client, urls: Vec<(UUID, Link)>, counter: Arc<AtomicI32>, headers: HeaderMap) -> Result<Vec<(UUID, Body)>> {
    let concurrency_limit = 5;
    let delay_between_requests = Duration::from_millis(50);

    let results = stream::iter(urls.into_iter().map(|target| {
        let g_move = counter.clone();
        let h_move = headers.clone(); //well i would arc it but the api wants an owned one, so clone it is ._.
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

#[instrument(skip_all, parent = &tracing::Span::current())]
async fn extract_has_split(bodies: Vec<(UUID, Body)>) -> Result<Vec<(UUID, Body, RssPresence)>> {

    let contents = tokio::task::spawn_blocking(move || {
        let detector = Arc::new(RSSPhaseOneDetector::new());

        let active_span = tracing::Span::current();
        
        let extracted = bodies.into_par_iter().map(|body| {
            let g = active_span.enter();
            
            let filtered_body = extract_all_text(body.1);
            let ae = detector.detect_rss_potential(filtered_body.as_str());
            
            
            drop(g);
            return (body.0, filtered_body, ae)
        }).collect::<Vec<_>>();
        
        extracted
    }).await?;
    
    Ok(contents)
}
#[instrument(skip(bodies), parent = &tracing::Span::current())]
async fn extract_8k_links(bodies: Vec<(UUID,Body)>) -> Result<Vec<(UUID,Link)>> {
    info!("test for span loc");
    let table_selector = Arc::new(Selector::parse("table").unwrap());
    let row_selector = Arc::new(Selector::parse("tr").unwrap());
    let cell_selector = Arc::new(Selector::parse("td").unwrap());
    let link_selector = Arc::new(Selector::parse("a").unwrap());

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



#[instrument(skip_all, parent = &tracing::Span::current())]
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


fn generate_domain_from_name(company_name: &str) -> String {
    company_name
        .to_lowercase()               // Convert to lowercase
        .replace(" ", "")              // Remove spaces
        .replace("'", "")              // Remove apostrophes
        .replace("&", "and")           // Replace & with "and"
}

#[instrument(skip_all, parent = &tracing::Span::current())]
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


#[instrument(skip_all)]
fn extract_all_text(html: String) -> String {
    let document = Html::parse_fragment(&html);
    let mut result = String::new();

    for node in document.tree {
        if let Some(text) = node.as_text() {
            let trimmed_text = text.trim();
            if !trimmed_text.is_empty() {
                result.push_str(trimmed_text);
                result.push(' ');
            }
        }
    }

    result.trim().to_string()
}
