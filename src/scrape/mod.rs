#![feature(async_closure)]

mod browser;
pub mod rss_inference;
pub mod rss_presence;
mod spoof;

use std::result;
use rayon::prelude::*;

use anyhow::Result;
use apalis::prelude::Data;
use chrono::{DateTime, Utc};
use feed_rs::parser;
use futures::{stream, StreamExt};
use lazy_static::lazy_static;
use reqwest::header::{HeaderMap, LAST_MODIFIED};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio::{join, sync};
use tracing::{error, info, trace, warn};
use tracing::instrument;

use crate::core::{Body, Core, Link, UUID};

use crate::billboard::perfmon::{PerfmonError, PerfmonTask};
use crate::core::database::FilingDocument;
use crate::scrape::rss_inference::{Classification, Inference, LLMInference};
use crate::scrape::rss_presence::{RSSPhaseOneDetector, RssPresence};
use scraper::selectable::Selectable;
use scraper::{Html, Selector};
use thiserror::Error;
use tokio::sync::Mutex;
use crate::scrape::FetchBodyError::BadStatusError;

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

#[derive(Serialize, Deserialize)]
pub struct RSSTask {}

#[derive(Clone)]
pub struct RSSService {
    core: Arc<Core>,
    
    last_6k: Arc<Mutex<DateTime<Utc>>>,
    last_8k: Arc<Mutex<DateTime<Utc>>>,
}



#[derive(Error, Debug)]
enum ScraperError {
    #[error(transparent)]
    GenericError(#[from] anyhow::Error)
}

struct UUIDAndLink {
    uuid: UUID,
    link: String,
}

#[instrument(skip(core))]
pub async fn run_rss(
    _task: PerfmonTask,
    core: Data<RSSService>,
    //buysell: Data<Arc<RedisStorage<BuySellTask>>>,
) -> std::result::Result<(), PerfmonError> {
    
    trace!("running rss task");
    core.scan().await?;
    trace!("done!");

    Ok(())
}

impl RSSService {
/*
    async fn check_ready(&self) -> Result<bool> {
        let headers = spoof::generate_headers();
        let (response6k, response8k) = join!(
            self.core.client.head(SEC_6K_LINK).headers(headers.clone()).send(),
            self.core.client.head(SEC_8K_LINK).headers(headers).send()
        );
        let response6k = response6k?;
        let response8k = response8k?;
        
        info!("{:#?}",response8k.headers());

        let last_modified_6k = response6k.headers().get(LAST_MODIFIED)
            .ok_or_else(|| anyhow::anyhow!(LAST_MODIFIED))?
            .to_str()?
            .parse::<DateTime<Utc>>()?;
        let last_modified_8k = response8k.headers().get("last-modified")
            .ok_or_else(|| anyhow::anyhow!("Missing last-modified header for 8-K"))?
            .to_str()?
            .parse::<DateTime<Utc>>()?;

        let is_new_content_6k = last_modified_6k > *self.last_6k.lock().await;
        let is_new_content_8k = last_modified_8k > *self.last_8k.lock().await;

        // Update stored last modified dates if new content is found
        if is_new_content_6k {
            *self.last_6k.lock().await = last_modified_6k;
        }
        if is_new_content_8k {
            *self.last_8k.lock().await = last_modified_8k;
        }

        Ok(is_new_content_6k || is_new_content_8k)
    }
    */
    #[instrument(skip(self))]
    async fn pull_body(&self) -> Result<(String, String, StatusCode, StatusCode, HeaderMap)> {
        let headers = spoof::generate_headers();
        let future6k = self
            .core
            .client
            .get(SEC_6K_LINK)
            .headers(headers.clone())
            .send();
        let future8k = self
            .core
            .client
            .get(SEC_8K_LINK)
            .headers(headers.clone())
            .send();
        
        let (response6k, response8k) = join!(future6k, future8k);
        let (resp6k, resp8k) = (response6k?, response8k?);
        let (status6k, status8k) = (resp6k.status(), resp8k.status());

        let (body6k, body8k) = (resp6k.text().await?, resp8k.text().await?);

        Ok((body6k, body8k, status6k, status8k, headers))
    }

    #[instrument(skip(self, body6k, body8k))]
    async fn merge_feeds_and_collect(
        &self,
        body6k: String,
        body8k: String,
    ) -> Result<Vec<UUIDAndLink>> {
        //read the main feeds
        let (feed6k, feed8k) = (
            parser::parse(body6k.as_bytes())?,
            parser::parse(body8k.as_bytes())?,
        );
        let (size6k, size8k) = (feed6k.entries.len(), feed8k.entries.len());
        // Combine entries from both feeds

        trace!("count {}, {}", feed6k.entries.len(), feed8k.entries.len());


        let mut feed_urls = Vec::new();
        feed_urls.extend(feed6k.entries);
        feed_urls.extend(feed8k.entries);


        let unseen_uuids = stream::iter(feed_urls)
            .filter_map(|mut individual_entry| async move {
                let id_copy = individual_entry.id;
                let out = self.core.db.get_filing_document(&id_copy).await;

                if let Err(e) = out {
                    return Some(Err(e));
                }
                if out.unwrap().is_some() {
                     return None;
                }
                let ved = {
                    let z = individual_entry.links.remove(0);
                    let rr = z.href;

                    Some(Ok(UUIDAndLink {
                        uuid: id_copy,
                        link: rr,
                    }))
                };
                return ved;
            })
            .collect::<Vec<Result<UUIDAndLink>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<UUIDAndLink>>>();

        unseen_uuids
    }

    #[instrument(skip(self, headers, unseen_ids, counter))]
    async fn visit_deeplink(
        &self,
        headers: &HeaderMap,
        unseen_ids: Vec<UUIDAndLink>,
        counter: Arc<AtomicI32>,
    ) -> Result<Vec<(UUID, Body)>> {
        let max_progress = unseen_ids.len() as u32;
        let hub_counter = counter.clone();

        let (killsig_tx, mut killsig_rx) = sync::oneshot::channel::<()>();
        let ref_client = &self.core.client; // Need this to prevent client from being moved into the first map

        let unseen_links = unseen_ids
            .into_iter()
            .map(|tuple| (tuple.uuid, tuple.link))
            .collect::<Vec<(UUID, Link)>>();

        let base_headers = headers.clone();

        let hub_bodies = join!(
            async { loop {
                    match killsig_rx.try_recv() {
                        Ok(()) => {
                            break;
                        }
                        _ => {
                            let g = format!(
                                "scanned [{}/{}] uniques...",
                                hub_counter.load(Ordering::Relaxed),
                                max_progress
                            );
                            info!("{}",g);
                            sleep(Duration::from_secs(3)).await;
                        }
                    }
            } },
            async {
                let out =
                    fetch_body_bulk(ref_client, unseen_links, hub_counter.clone(), base_headers)
                        .await;
                killsig_tx.send(()).expect("oops");
                return out;
            }
        )
            .1?;

        info!(
            "visited [{}/{}] unique entries...",
            hub_counter.load(Ordering::Relaxed),
            max_progress
        );
        Ok(hub_bodies)
    }

    #[instrument(skip(self, headers, unseen_ids, counter))]
    async fn visit_intermediaries(
        &self,
        headers: &HeaderMap,
        unseen_ids: Vec<UUIDAndLink>,
        counter: Arc<AtomicI32>,
    ) -> Result<Vec<(UUID, Body)>> {
        let max_progress = unseen_ids.len() as u32;
        let hub_counter = counter.clone();

        let (killsig_tx, mut killsig_rx) = sync::oneshot::channel::<()>();
        let ref_client = &self.core.client; // Need this to prevent client from being moved into the first map

        let unseen_links = unseen_ids
            .into_iter()
            .map(|tuple| (tuple.uuid, tuple.link))
            .collect::<Vec<(UUID, Link)>>();

        let base_headers = headers.clone();

        let hub_bodies = join!(
            async { loop {
                    match killsig_rx.try_recv() {
                        Ok(()) => {
                            break;
                        }
                        _ => {
                            let g = format!(
                                "scanned [{}/{}] uniques...",
                                hub_counter.load(Ordering::Relaxed),
                                max_progress
                            );
                            info!("{}",g);
                            sleep(Duration::from_secs(3)).await;
                        }
                    }
            } },
            async {
                let out =
                    fetch_body_bulk(ref_client, unseen_links, hub_counter.clone(), base_headers)
                        .await;
                killsig_tx.send(()).expect("oops");
                return out;
            }
        )
        .1?;

        info!(
            "visited [{}/{}] unique entries...",
            hub_counter.load(Ordering::Relaxed),
            max_progress
        );
        Ok(hub_bodies)
    }

    #[instrument(skip(self))]
    async fn scan(&self) -> Result<()> {
/*
        info!("checking ready");
        if !self.check_ready().await? {
            info!("no new content since we last peeked");
            return Ok(());
        }*/


        info!("new content, scanning");
        let (body6k, body8k, status6k, status8k, headers) = self.pull_body().await?;
        let status = status6k.is_success() && status8k.is_success();
        if !status {
            error!(
                "failed to pull edgar, status codes [{}] [{}]",
                status6k, status8k
            );
        }
        

        //Merge feeds
        let unseen_ids = self.merge_feeds_and_collect(body6k, body8k).await?;
        info!(
            "found {} unique documents in edgar, status codes [{}] [{}]",
            unseen_ids.len(),
            status6k,
            status8k
        );

        if unseen_ids.len() == 0 {
            trace!("no new entries found, back to sleep");

            return Ok(());
        }

        info!("visiting edgar intermediary pages");
        let counter = Arc::new(AtomicI32::new(0));
        let intermediary_bodies: Vec<(UUID, Body)> = self
            .visit_intermediaries(&headers, unseen_ids, counter)
            .await?;

        info!("extracting 8-k links from intermediaries:");
        let intermediary_links: Vec<(UUID, Link)> = extract_8k_links(intermediary_bodies).await?;

        info!("visiting 8-k pages");
        let filings_counter = Arc::new(AtomicI32::new(0));
        let filing_bodies: Vec<(UUID,Body)> =
            fetch_body_bulk(&self.core.client, intermediary_links, filings_counter, headers).await?;
        let everything = extract_has_split(filing_bodies).await?;

        trace!("scanning 8-k pages for split status");
        let all_filings = everything
            .into_iter()
            .map(|tuple| {
                let (uuid, body, presence) = tuple;
                let now = Utc::now();
                let filing_document = FilingDocument::new(uuid, now.into(), presence, None, body);

                filing_document
            })
            .collect::<Vec<FilingDocument>>();

        info!("collecting interesting documents");
        let mut key_documents = all_filings
            .iter()
            .filter(|d| d.is_split.0)
            .map(|d| d.clone()) //todo stop being lazy
            .collect::<Vec<FilingDocument>>();

        info!("pushing filings to db");
        self.core.db.push_filing_documents(all_filings).await?;

        info!("candidate count: {}", key_documents.len());
        let mut split = 0;

        info!("running inference");
        let inference = LLMInference::new(&self.core.client, &self.core.cfg.gpt_key);

        for document in key_documents.iter_mut() {
            let inference_data = inference.infer(&document.body_contents).await?;
            if inference_data.classification == Classification::RoundUp {
                split += 1;
            }

            //update doc
            document.post_inference = Some(inference_data);
        }

        info!("pushing inferred filings to db");
        for document in key_documents.into_iter() {
            self.core.db.update_filing_document(document).await?;
        }

        info!("real split count: {}", split);

        Ok(())
    }

    pub fn new(core: Arc<Core>) -> Self {
        Self {
            core,
            last_6k: Arc::new(Mutex::new(DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap().with_timezone(&Utc))),
            last_8k: Arc::new(Mutex::new(DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap().with_timezone(&Utc)))
        }
    }
    
}

#[derive(Debug, Error)]
enum FetchBodyError {
    #[error(transparent)]
    GenericError(#[from] anyhow::Error),

    #[error(transparent)]
    HttpError(#[from] reqwest::Error),
    
    #[error("bad status code: {0}")]
    BadStatusError(StatusCode)
}

#[instrument(skip_all)]
async fn fetch_body_bulk(
    client: &Client,
    urls: Vec<(UUID, Link)>,
    counter: Arc<AtomicI32>,
    headers: HeaderMap,
) -> Result<Vec<(UUID, Body)>> {
    let concurrency_limit = 5;
    let delay_between_requests = Duration::from_millis(50);

    let results = stream::iter(urls.into_iter().map(|target| {
        let g_move = counter.clone();
        let h_move = headers.clone(); //well i would arc it but the api wants an owned one, so clone it is ._.
        async move {
            let response = client.get(target.1).headers(h_move).send().await?;
            if !response.status().is_success() {
                return Err(anyhow::anyhow!("Request failed with status: {}", response.status()));
            }            
            let body = response.text().await?;
            g_move.fetch_add(1, Ordering::Relaxed); //increment progress bar
            sleep(delay_between_requests).await;

            return Ok((target.0, body));
        }
    }))
    .buffer_unordered(concurrency_limit) //io concurrency
    .collect::<Vec<_>>() // Collect results into a Vec
    .await;

    Ok(results
        .into_iter()
        .filter_map(|i: anyhow::Result<_>| i.ok())
        .collect::<Vec<_>>())
}

#[instrument(skip_all, parent = &tracing::Span::current())]
async fn extract_has_split(bodies: Vec<(UUID, Body)>) -> Result<Vec<(UUID, Vec<Body>, RssPresence)>> {
    let contents = tokio::task::spawn_blocking(move || {
        let detector = Arc::new(RSSPhaseOneDetector::new());

        let active_span = tracing::Span::current();

        let extracted = bodies
            .into_par_iter()
            .map(|body| {
                let g = active_span.enter();

                let filtered_body = preprocess_deep_body(body.1).expect("oops");
                let (presence, filtered_bodies) = detector.detect_rss_potential(filtered_body);

                drop(g);
                return (body.0, filtered_bodies, presence);
            })
            .collect::<Vec<_>>();

        extracted
    })
    .await?;

    Ok(contents)
}
#[instrument(skip(bodies), parent = &tracing::Span::current())]
async fn extract_8k_links(bodies: Vec<(UUID, Body)>) -> Result<Vec<(UUID, Link)>> {
    let results = stream::iter(bodies)
        .map(|body| {
            async move {
                let result = extract_deep_link_from_intermediary_body(body.1).await;
                Ok((body.0, result.unwrap()))
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



lazy_static! {
    static ref TABLE_SELECTOR: Selector = Selector::parse("table").unwrap();
    static ref ROW_SELECTOR: Selector = Selector::parse("tr").unwrap();
    static ref CELL_SELECTOR: Selector = Selector::parse("td").unwrap();
    static ref LINK_SELECTOR: Selector = Selector::parse("a").unwrap();
    static ref BODY_SELECTOR: Selector = Selector::parse("body").unwrap();
}



//TODO this is much more brittle and we should test it
#[instrument(skip_all, parent = &tracing::Span::current())]
async fn extract_deep_link_from_intermediary_body(body: String) -> Result<String> {
    #[derive(Error,Debug)]
    enum ProcessIntermediaryError {
        #[error("the form linking table wasn't present")]
        TableNotPresentError,
        #[error("the text file linking row wasn't present")]
        LastRowNotPresentError,
        #[error("the text file linking cell wasn't present")]
        NoLinkError,
        #[error("the text file linking cell was present but had an empty link???")]
        EmptyLinkError,
        #[error("a link is present but didn't actually link anywhere??? (no href)")]
        NoHrefError,
        #[error(transparent)]
        GenericError(#[from] anyhow::Error),
    }
    
    let deeplink = Html::parse_document(&body)
        .select(&*TABLE_SELECTOR)
        .next()
        .ok_or(ProcessIntermediaryError::TableNotPresentError)?
        .select(&*ROW_SELECTOR)
        .last()
        .ok_or(ProcessIntermediaryError::LastRowNotPresentError)?
        .select(&*CELL_SELECTOR)
        .nth(2)
        .ok_or(ProcessIntermediaryError::NoLinkError)?
        .select(&*LINK_SELECTOR)
        .next()
        .ok_or(ProcessIntermediaryError::EmptyLinkError)?
        .value()
        .attr("href")
        .ok_or(ProcessIntermediaryError::NoHrefError)
        .map(|s| format!("https://www.sec.gov{}", s))?;
    
    Ok(deeplink)
}

#[instrument(skip_all)]
fn preprocess_deep_body(html: String) -> Result<Vec<String>> {
    #[derive(Error,Debug)]
    enum PreprocessError {
        #[error("somehow, the body of the final txt file is not present!")]
        NoBodyError,
        #[error(transparent)]
        GenericError(anyhow::Error)
    }
    
    let mut storage: Vec<String> = Vec::new();
    for bk in extract_html_blocks(&*html) {
        let body_safe = Html::parse_document(bk)
            .select(&*BODY_SELECTOR)
            .next()
            .ok_or(PreprocessError::NoBodyError)?
            .text()
            .map(|text| text.trim())
            .filter(|trimmed_text| !trimmed_text.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        
        storage.push(body_safe);
    }
    
    
    Ok(storage )
}

fn extract_html_blocks(input: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut offset = 0;
    let lower_input = input.to_lowercase();
    while let Some(start_pos) = lower_input[offset..].find("<html") {
        let absolute_start = offset + start_pos;
        if let Some(rel_end) = lower_input[absolute_start..].find("</html") {
            let absolute_end = absolute_start + rel_end + "</html>".len();
            results.push(&input[absolute_start..absolute_end]);
            offset = absolute_end;
        } else {
            break;
        }
    }

    results
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::load_data;
    use crate::scrape::spoof::generate_headers;
    use std::fs::File;
    use std::io::Read;
    use std::sync::atomic::AtomicI32;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_visit_intermediaries() {
        let core = Arc::new(load_data().await.expect("oops"));
        let context = RSSService {
            core: core.clone(),
            last_6k: Arc::new(Mutex::new(DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap().with_timezone(&Utc))),
            last_8k: Arc::new(Mutex::new(DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap().with_timezone(&Utc))),
        };

        let headers = generate_headers();
        let unseen_ids = vec![ //TODO iXBRL is a nuisance that i must sort out
                               //TODO extract stock ticker from the dei:TradingSymbol, instead of the inference
            UUIDAndLink { uuid: "uuid1".to_string(), link: "https://www.sec.gov/Archives/edgar/data/1497253/000095017025006819/0000950170-25-006819-index.htm".to_string() },
        ];
        let counter = Arc::new(AtomicI32::new(0));

        let result = context.visit_intermediaries(&headers, unseen_ids, counter.clone()).await.expect("oops");
        let result = extract_8k_links(result).await.expect("oops");

        let veco = result
            .iter()
            .map(|v| UUIDAndLink{uuid: v.0.clone(), link: v.1.clone()})
            .collect::<Vec<UUIDAndLink>>();

        let visited = context.visit_intermediaries(&headers, veco, counter.clone()).await.expect("TODO: panic message");

        for v in visited {
            println!("{}", v.1)
        }
    }

    #[test]
    fn test_extract_all_text() {
        
        println!("TEST");
        let mut file = File::open("assets/test/bioline_6k_deep.txt").expect("Unable to open file");
        let mut contents = String::new();
        file.read_to_string(&mut contents).expect("Unable to read file");

        let result = preprocess_deep_body(contents);
        println!("{:?}", result);
        
    }

}
/*#[tokio::test]
async fn test_scan_unique_with_429() {
    use httpmock::MockServer;
    use httpmock::Method::GET;
    use std::sync::atomic::AtomicI32;
    use std::sync::Arc;

    // Start a local mock server
    let server = MockServer::start();

    // Create a mock for the 6-K link that returns a 429 status code
    let _mock6k = server.mock(|when, then| {
        when.method(GET)
            .path("/cgi-bin/browse-edgar")
            .query_param("action", "getcurrent")
            .query_param("type", "6-K");
        then.status(429);
    });

    // Create a mock for the 8-K link that returns a 200 status code with a valid feed
    let _mock8k = server.mock(|when, then| {
        when.method(GET)
            .path("/cgi-bin/browse-edgar")
            .query_param("action", "getcurrent")
            .query_param("type", "8-K");
        then.status(200)
            .body(include_str!("../../assets/test/valid_feed.xml"));
    });

    let core = Arc::new(load_data().await.expect("oops"));
    let context = RssContext {
        core: Data::new(core.clone()),
    };

    let headers = generate_headers();
    let unseen_ids = vec![
        UUIDAndLink { uuid: "uuid1".to_string(), link: format!("{}/cgi-bin/browse-edgar?action=getcurrent&type=6-K", server.base_url()) },
        UUIDAndLink { uuid: "uuid2".to_string(), link: format!("{}/cgi-bin/browse-edgar?action=getcurrent&type=8-K", server.base_url()) },
    ];
    let counter = Arc::new(AtomicI32::new(0));

    let result = context.visit_intermediaries(&headers, unseen_ids, counter.clone()).await;

    assert!(result.is_err());
    if let Err(e) = result {
        assert!(e.to_string().contains("Request failed with status: 429"));
    }
}
*/