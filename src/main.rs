#![warn(clippy::str_to_string)]

extern crate pretty_env_logger;

use crate::billboard::perfmon::*;
use crate::billboard::deploy;
use crate::scrape::{run_rss, RSSService};
use tower::retry::{RetryLayer};
use apalis::prelude::{MemoryStorage, Monitor, Storage, WorkerBuilder, WorkerBuilderExt, WorkerFactoryFn};
use apalis_cron::{CronStream, Schedule};
use core::Core;
use poise::{serenity_prelude as serenity, Framework};
use serenity::all::{EventHandler, GuildId, RatelimitInfo};
use serenity::async_trait;
use std::error::Error;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use apalis::layers::tracing::OnFailure;
use apalis_redis::RedisStorage;
use tokio::sync::mpsc::{channel, Sender};
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::retry::backoff::{ExponentialBackoff, ExponentialBackoffMaker};
use tracing::{info, trace, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{prelude::*, registry::Registry};
use tracing_subscriber::EnvFilter;
use venator::Venator;
use crate::buysell::BuyTask;
use crate::discord2::{DiscordService, DiscordTask};

mod billboard;
mod buysell;
mod core;
mod logging;
mod scrape;
mod discord2;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    //LOGGING STUFF
    let filter = EnvFilter::default()
        .add_directive("splitflow=info".parse()?)
        .add_directive("tokio=warn".parse()?)
        .add_directive("tokio_cron_scheduler=trace".parse()?)
        .add_directive("apalis=warn".parse()?)
        .add_directive("serenity=warn".parse()?);
    

    let subscriber = Registry::default()
        .with(Venator::default())
        .with(filter)
        .with(tracing_subscriber::fmt::Layer::default().compact());

    tracing::subscriber::set_global_default(subscriber).expect("setting global default failed");
    trace!("loaded logging");

    //MAIN DATABASE STUFF
    let core = Arc::new(core::load_data().await?);
    info!("loaded core");

    //DISCORD STUFF
    let (ready_tx, ready_rx) = channel::<u8>(1);
    let token: String = { core.cfg.discord_token.clone() };
    let intents = serenity::GatewayIntents::non_privileged()
        | serenity::GatewayIntents::MESSAGE_CONTENT
        | serenity::GatewayIntents::GUILD_MESSAGES
        | serenity::GatewayIntents::GUILDS;

    let framework_core = core.clone();
    let framework = Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![deploy()],
            ..Default::default()
        })
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                Ok(framework_core)
            })
        })
        .build();

    let mut client = serenity::ClientBuilder::new(token, intents)
        .framework(framework)
        .event_handler(Handler {
            is_loop_running: Default::default(),
            sender: ready_tx,
        })
        .await?;

    //ready_rx.recv().await;
    info!("loaded discord..");

    let redis_url = core.cfg.redis_url.clone();
    let conn = tokio::time::timeout(Duration::from_secs(5), apalis_redis::connect(redis_url))
        .await
        .expect("Connection timed out")
        .expect("Could not connect");

    info!("loaded redis..");


    //discord service (we can set up subuser messaging through this later)
    let discord_service: DiscordService = DiscordService::new(client.http.clone(), core.cfg.announcement_channel.clone());
    let discord_queue: MemoryStorage<DiscordTask> = MemoryStorage::new();
    let announcement_worker = WorkerBuilder::new("discord_announcements")
        .concurrency(2)
        .data(discord_service)
        .backend(discord_queue.clone())
        .build_fn(discord2::process_task);

    info!("loaded discord worker..");

    //buy service
    let buy_queue: RedisStorage<BuyTask> = RedisStorage::new(conn);

    

    //performance monitor
    let perfmon_worker = WorkerBuilder::new("perfmon")
        .enable_tracing()
        .layer(LoadShedLayer::new())
        .layer(ConcurrencyLimitLayer::new(1))
        .data(core.clone())
        .data(client.http.clone())
        .backend(CronStream::new(Schedule::from_str("1/7 * * * * *")?))
        .build_fn(run_perfmon);

    let rss_service = RSSService::new(core.clone(), buy_queue, discord_queue);

    //rss scraper
    let rss_worker = WorkerBuilder::new("scraper")
        .enable_tracing()
        .layer(LoadShedLayer::new())
        .layer(ConcurrencyLimitLayer::new(1))
        .data(rss_service)
        .backend(CronStream::new(Schedule::from_str("0 */20 * * * *")?))
        .build_fn(run_rss);


    
    //discord processors
    let discord_future = client.start();
    let monitor_future = Monitor::new()
        .register(rss_worker)
        .register(perfmon_worker)
        .register(announcement_worker)
        .shutdown_timeout(Duration::from_secs(10))
        .run_with_signal(tokio::signal::ctrl_c());


    info!("assembled workers..");

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

struct Handler {
    is_loop_running: AtomicBool,
    sender: Sender<u8>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ratelimit(&self, data: RatelimitInfo) {
        warn!(
            "being ratelimited. limit: {} lm: {:?}, timeout: {}",
            data.limit,
            data.method,
            data.timeout.as_secs()
        )
    }

    async fn cache_ready(&self, _ctx: serenity::Context, _guilds: Vec<GuildId>) {
        if !self.is_loop_running.load(Ordering::Relaxed) {
            self.sender.send(1).await.expect("TODO: panic message");

            self.is_loop_running.swap(true, Ordering::Relaxed);
        }
    }
}

pub type DynError = Box<dyn Error + Send + Sync>;
pub type DynResult<T> = Result<T, DynError>;
pub type DynNothing = DynResult<()>;
pub type PoiseContext<'a> = poise::Context<'a, Arc<Core>, DynError>;

const AURIIUM_NAME: &str = "auriium's software";
