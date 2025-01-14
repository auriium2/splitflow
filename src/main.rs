#![warn(clippy::str_to_string)]

use std::error::Error;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use chrono::{DateTime, Utc};

use clokwerk::{AsyncScheduler, Job, Scheduler, TimeUnits};
use clokwerk::Interval::Wednesday;
use clokwerk::timeprovider::ChronoTimeProvider;
use poise::{serenity_prelude as serenity, Framework};
use serenity::all::{EventHandler, GuildId, RatelimitInfo};
use serenity::async_trait;
use tokio::join;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use rand::random;

use crate::billboard::{console, deploy, perfmon, NEUTRAL_CONSOLE_BB, RSS_CONSOLE_BB};
use crate::billboard::console::{Console, ConsoleCommand, ConsoleMessage, DateCommand};
use crate::billboard::perfmon::*;
use core::Core;
use crate::logging::ConsoleLogger;
use crate::scrape::RSSCommand;

mod billboard;
mod scrape;
mod buysell;
mod logging;
mod core;

extern crate pretty_env_logger;
#[macro_use] 
extern crate log;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    console_subscriber::init();
    
    //setup log forwarding
    /*let console_rx = {
        let (tx,rx): (Sender<ConsoleCommand<u8>>, Receiver<ConsoleCommand<u8>>) = mpsc::channel(10);
        let logger = ConsoleLogger::new(tx);
        log::set_boxed_logger(Box::new(logger))?;
        
        rx
    };*/
    
   pretty_env_logger::init();

    


    let core = Arc::new(core::load_data()?);
    let (start_tx, mut start_rx) = mpsc::channel(1);

    //MESSAGES AND SCHEDULING
    let (perfmon_tx,perfmon_rx): (Sender<PerfmonCommand>, Receiver<PerfmonCommand>) = mpsc::channel(10);
    let (rss_tx,rss_rx): (Sender<RSSCommand>, Receiver<RSSCommand>) = mpsc::channel(1);
    
    let (neutral_con_tx, neutral_con_rx): (Sender<DateCommand>, Receiver<DateCommand>) = mpsc::channel(50);
    let (rss_con_tx,rss_con_rx): (Sender<DateCommand>, Receiver<DateCommand>) = mpsc::channel(50);

    let main_neutral_con_tx_copy = neutral_con_tx.clone();
    
    async fn heartbeat_billboard(perfmon_tx: Sender<PerfmonCommand>, console_tx: Sender<DateCommand>, rss_tx: Sender<DateCommand>) -> () {
        console_tx.send(ConsoleCommand::Print(ConsoleMessage::new("hi".to_string() + " " + &*random::<u8>().to_string()), false)).await.expect("oops");

        let v = join!(
            perfmon_tx.send(PerfmonCommand::Tick),
            console_tx.send(ConsoleCommand::Tick),
            rss_tx.send(ConsoleCommand::Tick)
        );
        
        info!("sent console tick!");
        if let Err(why) = v.0 {
            log::error!("Error ticking perfmon: {why:?}")
        }

        if let Err(why2) = v.1 {
            log::error!("Error ticking console: {}", why2.to_string())
        }
    }

    async fn heartbeat_rssfeed(rss_console_tx: Sender<RSSCommand>) -> () {
        let _ = rss_console_tx.try_send(RSSCommand::RunProcess); // i am BUSY mother fu
    }
    let three = rss_con_tx.clone();
    let mut scheduler: AsyncScheduler<Utc, ChronoTimeProvider> = AsyncScheduler::with_tz(Utc);
    scheduler
        .every(18.seconds())
        .run(move || heartbeat_billboard(perfmon_tx.clone(), neutral_con_tx.clone(), three.clone()));
    scheduler
        .every(40.seconds())
        .run(move || heartbeat_rssfeed(rss_tx.clone()));
    
     tokio::spawn(async move {
        let out = start_rx.recv().await;
        if out.is_none() {
            log::error!("Something went wrong waiting for start..")
        }
        log::info!("Scheduler ready!");
        
        loop {
            scheduler.run_pending().await;
            sleep(Duration::from_millis(100)).await;
        }
    });

    //DISCORD STUFF
    let token: String = { core.config.discord_token.clone() };
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
        .event_handler(Handler{ is_loop_running: Default::default(), sender: start_tx })
        .await?;
    
    let rss_core = core.clone();
    let perfmon_core = core.clone();
    let perfmon_client = client.http.clone();


    //discord processors
    {
        let core = core.clone();
        let client = client.http.clone();
        tokio::spawn(async {
            Console::new(NEUTRAL_CONSOLE_BB, "DEBUG", core, client, neutral_con_rx).task().await
        });
    }
    {   
        let core = core.clone();
        let client = client.http.clone();
        tokio::spawn(async {
            Console::new(RSS_CONSOLE_BB, "RSS", core, client, rss_con_rx).task().await
        });
    }
    
    tokio::spawn(async {
        task_perfmon(perfmon_core, perfmon_client, perfmon_rx).await;
    });
  
    tokio::spawn(async {
        scrape::RSSTask::new(rss_core, rss_rx, rss_con_tx, )?.run().await;
        let e = scrape::task_update_rss(rss_core, rss_rx, rss_con_tx).await;
        
        if e.is_err() {
            log::error!("Error processing rss: {:?}", e.err().unwrap())
        }
        
        return
    });

    main_neutral_con_tx_copy.send(ConsoleCommand::Print(ConsoleMessage::new_str("[INFO] Systems ok!"), false)).await?;
    log::info!("Systems ok!");
    
    client.start().await.unwrap();
    Ok(())
}



struct Handler {
    is_loop_running: AtomicBool,
    sender: Sender<i8>
}

#[async_trait]
impl EventHandler for Handler {
    async fn ratelimit(&self, data: RatelimitInfo) {
        warn!("being ratelimited. limit: {} lm: {:?}, timeout: {}", data.limit, data.method, data.timeout.as_secs())
    }


    async fn cache_ready(&self, _ctx: serenity::Context, _guilds: Vec<GuildId>) {
        if !self.is_loop_running.load(Ordering::Relaxed) {
            self.sender.send(1).await.expect("TODO: panic message");
            
            self.is_loop_running.swap(true, Ordering::Relaxed);
        }
    }
}

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
pub type DynResult<T> = Result<T, DynError>;
pub type DynNothing = DynResult<()>;
pub type PoiseContext<'a> = poise::Context<'a, Arc<Core>, DynError>;

const AURIIUM_NAME: &str = "auriium's software";