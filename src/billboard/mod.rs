use poise::CreateReply;
use serde::{Deserialize, Serialize};
use serenity::all::{ChannelId, Colour, CreateEmbed, CreateEmbedFooter, CreateMessage, Timestamp};
use sled::IVec;

use crate::common::{DynNothing, PoiseContext};

pub mod perfmon;
pub mod console;

#[poise::command(prefix_command, slash_command)]
pub async fn set_alerted(ctx: PoiseContext<'_>,) -> DynNothing {
    ctx.say("You shouldn't be able to see this").await?;
    Ok(())
}

#[poise::command(prefix_command, slash_command, subcommands("perfmon", "console"))]
pub async fn deploy(ctx: PoiseContext<'_>) -> DynNothing {
    ctx.say("You shouldn't be able to see this").await?;
    Ok(())
}
const PERFMON_BB: &[u8; 7] = b"perfmon";
#[poise::command(prefix_command, slash_command)]
pub async fn perfmon(ctx: PoiseContext<'_>) -> DynNothing {
    return deploy_generic(ctx, PERFMON_BB).await;
}

//don't change these without a good reason
pub const NEUTRAL_CONSOLE_BB: &[u8; 15] = b"neutral_console";
pub const RSS_CONSOLE_BB: &[u8; 11] = b"rss_console";

#[poise::command(prefix_command, subcommands("neutral", "rss"))]
pub async fn console(ctx: PoiseContext<'_>) -> DynNothing {
    ctx.say("You shouldn't be able to see this").await?;
    Ok(())
}
#[poise::command(prefix_command, slash_command)]
pub async fn neutral(ctx: PoiseContext<'_>) -> DynNothing {
    return deploy_generic(ctx, NEUTRAL_CONSOLE_BB).await;
}
#[poise::command(prefix_command, slash_command)]
pub async fn rss(ctx: PoiseContext<'_>) -> DynNothing {
    return deploy_generic(ctx, RSS_CONSOLE_BB).await;
}



#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct BillboardLocation {
    channel_id: u64,
    message_id: u64
}

impl Into<IVec> for BillboardLocation {
    fn into(self) -> IVec {
        let bytes = bincode::serialize(&self).unwrap();
        IVec::from(bytes)
    }
}

pub async fn deploy_generic<const N: usize>(ctx: PoiseContext<'_>, id: &[u8; N]) -> DynNothing {
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

    let sign_post = BillboardLocation { channel_id: message.channel_id.get(), message_id: message.id.get() };
    let db_output = ctx.data().discord_db.insert(id, sign_post);

    if let Some(inner) = db_output? {
        let old = bincode::deserialize::<BillboardLocation>(inner.as_ref()).unwrap();
        let old_channel = ChannelId::new(old.channel_id);
        old_channel.delete_message(ctx, old.message_id).await?;

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

