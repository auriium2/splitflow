use crate::billboard::{BillboardLocation, PERFMON_BB};
use crate::core::Core;
use serenity::all::{ChannelId, Colour, CreateEmbed, CreateEmbedFooter, EditMessage, Http, Timestamp};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub enum PerfmonCommand {
    Tick,
    Die
}

pub async fn task_perfmon(core: Arc<Core>, ctx: Arc<Http>, mut rx: Receiver<PerfmonCommand>) -> anyhow::Result<()> {
    while let Some(cmd) =  rx.recv().await {
        let opt = core.db.get_signpost(PERFMON_BB.to_string()).await?;

        match cmd {
            PerfmonCommand::Tick => {
                if opt.is_some() {
                    let contents = opt.unwrap();
                    let old_channel = ChannelId::new(contents.channel_id.parse()?);
                    let edit = old_channel.edit_message(&ctx, contents.message_id.parse::<u64>()?, EditMessage::new().embed(generate_perfmon_embed(true))).await;

                    if let Err(why) = edit {
                        error!("Error sending message: {why:?}");
                    };
                }
            }
            PerfmonCommand::Die => {
                if opt.is_some() {
                    let contents = opt.unwrap();
                    let old_channel = ChannelId::new(contents.channel_id.parse()?);
                    let edit = old_channel.edit_message(&ctx, contents.message_id.parse::<u64>()?, EditMessage::new().embed(generate_perfmon_embed(false))).await;

                    if let Err(why) = edit {
                        error!("Error sending message: {why:?}");
                    };
                }
            }
        }
    }
    
    Ok(())
   
}


fn generate_perfmon_embed(online: bool) -> CreateEmbed {
    let cpu_load = sys_info::loadavg().unwrap();
    let mem_use = sys_info::mem_info().unwrap();

    let c: Colour = {
        if online {
            Colour::from_rgb(120,255,120)
        } else {
            Colour::from_rgb(255,120,120)
        }
    };

    let embed = CreateEmbed::new()
        .color(c)
        .title(if online {"STRIDER | PERFMON [ ONLINE ]"} else {"STRIDER | PERFMON [ OFFLINE ]"})
        .field("CPU Load Average", format!("{:.2}%", cpu_load.one * 10.0), false)
        .field(
            "Memory Usage",
            format!(
                "{:.2} MB Free out of {:.2} MB",
                mem_use.free as f32 / 1000.0,
                mem_use.total as f32 / 1000.0
            ),
            false,
        )
        .footer(CreateEmbedFooter::new("auriium software"))
        .timestamp(Timestamp::now());

    embed
}
