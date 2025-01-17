use poise::CreateReply;
use serde::{Deserialize, Serialize};
use serenity::all::{ChannelId, Colour, CreateEmbed, CreateEmbedFooter, CreateMessage, Timestamp};

use crate::core::database::SignpostDocument;
use crate::{DynNothing, PoiseContext};

pub mod perfmon;
pub mod console;

#[poise::command(prefix_command, slash_command)]
pub async fn set_alerted(ctx: PoiseContext<'_>,) -> DynNothing {
    ctx.say("You shouldn't be able to see this").await?;
    Ok(())
}

#[poise::command(prefix_command, slash_command, subcommands("perfmon", "console", "toasts"))]
pub async fn deploy(ctx: PoiseContext<'_>) -> DynNothing {
    ctx.say("You shouldn't be able to see this").await?;
    Ok(())
}
#[poise::command(prefix_command, slash_command)]
pub async fn perfmon(ctx: PoiseContext<'_>) -> DynNothing {
    deploy_generic(ctx, PERFMON_BB.to_string()).await
}

//don't change these without a good reason
pub const NEUTRAL_CONSOLE_BB: &str = "neutral_console";
pub const RSS_CONSOLE_BB: &str = "rss_console";
const PERFMON_BB: &str = "perfmon";


#[poise::command(prefix_command, subcommands("neutral", "rss"))]
pub async fn console(ctx: PoiseContext<'_>) -> DynNothing {
    ctx.say("You shouldn't be able to see this").await?;
    Ok(())
}
#[poise::command(prefix_command, slash_command)]
pub async fn neutral(ctx: PoiseContext<'_>) -> DynNothing {
    return deploy_generic(ctx, NEUTRAL_CONSOLE_BB.to_string()).await;
}
#[poise::command(prefix_command, slash_command)]
pub async fn rss(ctx: PoiseContext<'_>) -> DynNothing {
    return deploy_generic(ctx, RSS_CONSOLE_BB.to_string()).await;
}

static TOASTS: &str = "toasts";

#[poise::command(prefix_command, slash_command)]
pub async fn toasts(ctx: PoiseContext<'_>) -> DynNothing {

    let out = ctx.data().db.place_or_move_signpost(SignpostDocument::new(
        TOASTS.parse().unwrap(), ctx.channel_id().to_string(), 0.to_string()
    )).await?;

    if let Some(inner) = out {
        let channel_id: u64 = inner.channel_id.parse()?;
        let message_id: u64 = inner.message_id.parse()?;
        let old_channel = ChannelId::new(channel_id);
        old_channel.delete_message(ctx, message_id).await?;

        let reply = CreateReply::default()
            .ephemeral(true)
            .embed(CreateEmbed::new()
                .title("Strider | Toasts")
                .description("Successfully marked channel for toasts!"));

        ctx.send(reply).await?;

    } else {
        let reply = CreateReply::default()
            .ephemeral(true)
            .embed(CreateEmbed::new()
                .title("Strider | Billboards")
                .description("Successfully moved toasts to channel!"));

        ctx.send(reply).await?;
    };



    Ok(())
}


#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct BillboardLocation {
    channel_id: u64,
    message_id: u64
}

pub async fn deploy_generic(ctx: PoiseContext<'_>, id: String) -> DynNothing {
    let embed = CreateEmbed::new()
        .color(Colour::from_rgb(255,120,120))
        .title("STRIDER | LOADING...")
        .description("This will get replaced by a strider dynamic billboard soon!")
        .footer(CreateEmbedFooter::new("auriium software"))
        .timestamp(Timestamp::now());
    let message = ctx
        .channel_id()
        .send_message(ctx, CreateMessage::new().embed(embed))
        .await?;
    
    let out = ctx.data().db.place_or_move_signpost(SignpostDocument::new(
        id, message.channel_id.get().to_string(), message.id.to_string()
    )).await?;
    
    if let Some(inner) = out {
        let channel_id: u64 = inner.channel_id.parse()?;
        let message_id: u64 = inner.message_id.parse()?;
        let old_channel = ChannelId::new(channel_id);
        old_channel.delete_message(ctx, message_id).await?;

        let reply = CreateReply::default()
            .ephemeral(true)
            .embed(CreateEmbed::new()
                .title("Strider | Billboards")
                .description("Successfully created a billboard + deleted old billboard!"));
        
        ctx.send(reply).await?;

    } else {
        let reply = CreateReply::default()
            .ephemeral(true)
            .embed(CreateEmbed::new()
                .title("Strider | Billboards")
                .description("Successfully created a billboard!"));

        ctx.send(reply).await?;
    };
    
    

    Ok(())
}

