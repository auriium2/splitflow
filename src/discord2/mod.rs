use crate::buysell::{Action, BuyTask};
use crate::core::database::{CoreDB, SignpostDocument};
use crate::core::queue::QueueManager;
use crate::core::{SplitflowConfig, Ticker, APP_NAME};
use crate::discord2::util::{command_bad, command_good, command_mid, command_neutral};
use crate::scrape::RSSTask;
use anyhow::Result;
use apalis::prelude::Storage;
use async_trait::async_trait;
use poise::{serenity_prelude as serenity, Framework};
use poise::{CreateReply, PopArgument, SlashArgument};
use serde::{Deserialize, Serialize};
use serenity::all::{
    ChannelId, Colour, CreateEmbed, CreateEmbedFooter, CreateMessage, EventHandler,
    RatelimitInfo, Timestamp,
};
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{instrument, warn};

pub mod announce;
pub(crate) mod perfmon;
mod util;

//i hate poise with a passion
struct DiscordCore {
    queues: Arc<QueueManager>,
    db: Arc<CoreDB>,
}
type DiscordContext<'a> = poise::Context<'a, DiscordCore, anyhow::Error>;

/*
   Discord Commands Here
*/
#[poise::command(prefix_command, subcommands("buy", "pull"))]
async fn force(_: DiscordContext<'_>) -> Result<()> {
    Ok(())
}
#[poise::command(prefix_command, slash_command)]
async fn buy(ctx: DiscordContext<'_>, ticker: String) -> Result<()> {
    let real_ticker = Ticker::from_str(&ticker);
    if real_ticker.is_err() {
        ctx.send(command_bad(&format!(
            "err: {} is not a valid ticker",
            &ticker
        )))
        .await?;
        return Ok(());
    }
    let real_ticker = real_ticker.unwrap().0;

    let (task, notifier) = BuyTask::new_notify(Action::Buy, real_ticker);
    ctx.data().queues.push_buy(task).await?;
    ctx.send(command_neutral(&format!(
        "sent buy request for ticker {}",
        &ticker
    )))
    .await?;
    let timeout_duration = std::time::Duration::from_secs(30);
    match tokio::time::timeout(timeout_duration, notifier).await {
        Ok(_) => {
            ctx.send(command_good(&format!(
                "successfully bought ticker {}",
                &ticker
            )))
            .await?;
        }
        Err(_) => {
            ctx.send(command_mid(&format!(
                "timeout: buy request for ticker {} did not complete in time",
                &ticker
            )))
            .await?;
        }
    }

    Ok(())
}
#[poise::command(prefix_command, slash_command)]
async fn pull(ctx: DiscordContext<'_>) -> Result<()> {
    let (task, notifier) = RSSTask::new_notify();
    ctx.data()
        .queues
        .push_scan(task)
        .await
        .expect("impossible exception");
    ctx.send(command_neutral("sent a rss request!")).await?;
    let timeout_duration = std::time::Duration::from_secs(120);
    match tokio::time::timeout(timeout_duration, notifier).await {
        Ok(_) => {
            ctx.send(command_good("pull completed successfully!"))
                .await?;
        }
        Err(_) => {
            ctx.send(command_mid("timeout: pull didn't complete in time"))
                .await?;
        }
    }

    Ok(())
}

/*
   Billboard stuff here
*/
pub const PERFMON_BB: &str = "perfmon";

#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct BillboardLocation {
    channel_id: u64,
    message_id: u64,
}

#[poise::command(prefix_command, subcommands("perfmon"))]
async fn deploy(_: DiscordContext<'_>) -> Result<()> {
    Ok(())
}
#[poise::command(prefix_command, slash_command)]
async fn perfmon(ctx: DiscordContext<'_>) -> Result<()> {
    deploy_generic(ctx, PERFMON_BB.to_string()).await
}
async fn deploy_generic(ctx: DiscordContext<'_>, id: String) -> Result<()> {
    let embed = CreateEmbed::new()
        .color(Colour::from_rgb(255, 120, 120))
        .title("loading...")
        .description("this will get replaced by a splitflow dynamic billboard soon!")
        .footer(CreateEmbedFooter::new("auriium software"))
        .timestamp(Timestamp::now());
    let message = ctx
        .channel_id()
        .send_message(ctx, CreateMessage::new().embed(embed))
        .await?;

    let out = ctx
        .data()
        .db
        .place_or_move_signpost(SignpostDocument::new(
            id,
            message.channel_id.get().to_string(),
            message.id.to_string(),
        ))
        .await?;

    if let Some(inner) = out {
        let channel_id: u64 = inner.channel_id.parse()?;
        let message_id: u64 = inner.message_id.parse()?;
        let old_channel = ChannelId::new(channel_id);
        old_channel.delete_message(ctx, message_id).await?;

        let reply = CreateReply::default().ephemeral(true).embed(
            CreateEmbed::new()
                .title(APP_NAME)
                .description("successfully created a billboard + deleted old billboard!"),
        );

        ctx.send(reply).await?;
    } else {
        let reply = CreateReply::default().ephemeral(true).embed(
            CreateEmbed::new()
                .title(APP_NAME)
                .description("successfully created a billboard!"),
        );

        ctx.send(reply).await?;
    };

    Ok(())
}

/*
   Loading here
*/
#[instrument(skip_all)]
pub async fn load_discord(cfg: &SplitflowConfig, qm: Arc<QueueManager>, db: Arc<CoreDB>) -> Result<serenity::Client> {
    let intents = serenity::GatewayIntents::non_privileged()
        | serenity::GatewayIntents::MESSAGE_CONTENT
        | serenity::GatewayIntents::GUILD_MESSAGES
        | serenity::GatewayIntents::GUILDS;

    let framework = Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![deploy(), force()],
            ..Default::default()
        })
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                Ok(DiscordCore { queues: qm, db })
            })
        })
        .build();

    let client = serenity::ClientBuilder::new(&*cfg.discord_token, intents)
        .framework(framework)
        .event_handler(Handler {})
        .await?;

    Ok(client)
}

struct Handler {}

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
}
