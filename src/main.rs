#![warn(clippy::str_to_string)]

extern crate pretty_env_logger;

use crate::buysell::python::PythonAllService;
use crate::buysell::{buy_task, BuyTask};
use crate::core::database::{load_mongo_db, load_redis_conn};
use crate::core::load::{load_cfg, load_proxied_client, load_unproxied_client};
use crate::core::queue::QueueManager;
use crate::core::SplitflowConfig;
use crate::discord2::load_discord;
use crate::discord2::perfmon::PerfmonService;
use crate::scrape::{rss_task, RSSService, RSSTask};
use crate::util::MergedStorage;
use apalis::prelude::{
    MemoryStorage, Monitor, WorkerBuilder, WorkerBuilderExt, WorkerFactoryFn,
};
use apalis_cron::{CronStream, Schedule};
use apalis_redis::RedisStorage;
use chrono::Utc;
use discord2::announce;
use discord2::announce::{DiscordService, AnnounceTask};
use discord2::perfmon::perfmon_task;
use poise::serenity_prelude as serenity;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use apalis::layers::tracing::TraceLayer;
use sentry::ClientInitGuard;
use tokio::try_join;
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::retry::RetryLayer;
use tracing::{event, info, trace, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{prelude::*, registry::Registry};

mod buysell;
mod core;
mod discord2;
mod logging;
mod scrape;
mod util;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let _guard = load_logging()?;

    //CORE STUFF
    let cfg: SplitflowConfig = load_cfg().await?;
    let (conn, proxied_client, unproxied_client, db) = try_join!(
        load_redis_conn(&cfg),
        load_proxied_client(&cfg),
        load_unproxied_client(&cfg),
        load_mongo_db(&cfg)
    )?;
    let db = Arc::new(db);

    //QUEUE STUFF
    let rss_queue: MemoryStorage<RSSTask> = MemoryStorage::new();
    let buy_queue: RedisStorage<BuyTask> = RedisStorage::new(conn);
    let discord_queue: MemoryStorage<AnnounceTask> = MemoryStorage::new();
    let qm = Arc::new(QueueManager::new(
        buy_queue.clone(),
        discord_queue.clone(),
        rss_queue.clone(),
    ));

    let rss_cron = "1 */20 * * * *";
    //let rss_cron = "40 * * * * *";
    let rss_stream: CronStream<RSSTask, Utc> = CronStream::new(Schedule::from_str(rss_cron)?);
    let merged_rss_queue = MergedStorage::<RSSTask>::new(rss_queue, rss_stream);

    //DISCORD STUFF
    let mut discord_client = load_discord(&cfg, qm.clone(), db.clone()).await?;
    let discord_http = (&discord_client).http.clone();

    //discord service (we can set up subuser messaging through this later)
    let discord_service: DiscordService =
        DiscordService::new(discord_http.clone(), &*cfg.announcement_channel);
    let announcement_worker = WorkerBuilder::new("discord_announcements")
        .concurrency(2)
        .data(discord_service)
        .backend(discord_queue)
        .build_fn(announce::process_task);
    info!("loaded discord worker..");

    //performance monitor
    let perfmon_service: PerfmonService = PerfmonService::new(db.clone(), discord_http.clone());
    let perfmon_worker = WorkerBuilder::new("perfmon")
        .enable_tracing()
        .layer(LoadShedLayer::new())
        .layer(ConcurrencyLimitLayer::new(1))
        .data(perfmon_service)
        .backend(CronStream::new(Schedule::from_str("1/7 * * * * *")?))
        .build_fn(perfmon_task);

    let rss_service = RSSService::new(proxied_client, cfg.clone(), db.clone(), qm.clone());

    //rss scraper
    let rss_worker = WorkerBuilder::new("scraper")
        .enable_tracing()
        .layer(LoadShedLayer::new())
        .layer(ConcurrencyLimitLayer::new(1))
        .data(rss_service)
        .backend(merged_rss_queue)
        .build_fn(rss_task);

    //buy processor
    let py_server_svc = PythonAllService::new(unproxied_client, cfg.buyserver_url.clone());
    let buy_worker = WorkerBuilder::new("buysell")
        .enable_tracing()
        .data(py_server_svc)
        .backend(buy_queue)
        .build_fn(buy_task);

    //discord processors
    let discord_future = discord_client.start();
    let monitor_future = Monitor::new()
        .register(buy_worker)
        .register(rss_worker)
        .register(perfmon_worker)
        .register(announcement_worker)
        /*.on_event(|e| {
            tracing::info!("listener: {e:?}")
        })*/ //TODO pipe this to the Status Monitor Perfmonbot 2
        .shutdown_timeout(Duration::from_secs(10))
        .run_with_signal(tokio::signal::ctrl_c());

    info!("system OK");

    
    let jz = tokio::select! {
        _ = discord_future => {
            info!("Application tasks completed.");
        }
        _ = monitor_future => {
            info!("Received Ctrl+C, shutting down...");
        }
    };

    tokio::time::sleep(Duration::from_secs(4)).await;

    info!("Shutting down...");
    Ok(())
}

fn load_logging() -> anyhow::Result<ClientInitGuard> {
    let guard = sentry::init(("https://fcd01658de95c45347b2f688b0be014f@o4508741150113792.ingest.us.sentry.io/4508741156077568", sentry::ClientOptions {
        release: sentry::release_name!(),
        traces_sample_rate: 1.0, //TODO lower this in prod
        ..sentry::ClientOptions::default()
    }));

    //LOGGING STUFF
    let filter = EnvFilter::default()
        .add_directive("splitflow=info".parse()?)
        .add_directive("tokio=warn".parse()?)
        .add_directive("tokio_cron_scheduler=trace".parse()?)
        .add_directive("apalis=warn".parse()?)
        .add_directive("serenity=warn".parse()?);

    let subscriber = Registry::default()
        .with(ErrorLayer::default())
        .with(sentry_tracing::layer())
        .with(filter)
        .with(tracing_subscriber::fmt::Layer::default().compact());

    tracing::subscriber::set_global_default(subscriber).expect("setting global default failed");
    trace!("loaded logging");
    Ok(guard)
}
