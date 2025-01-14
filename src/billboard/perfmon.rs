use std::sync::Arc;
use serenity::all::{ChannelId, Colour, CreateEmbed, CreateEmbedFooter, EditMessage, Http, Timestamp};
use tokio::sync::mpsc::Receiver;
use crate::core::Core;
use crate::billboard::{BillboardLocation, PERFMON_BB};

pub enum PerfmonCommand {
    Tick,
    Die
}

pub async fn task_perfmon(core: Arc<Core>, ctx: Arc<Http>, mut rx: Receiver<PerfmonCommand>) {
    while let Some(cmd) =  rx.recv().await {
        let opt = core.discord_db.get(PERFMON_BB).unwrap();

        match cmd {
            PerfmonCommand::Tick => {
                if opt.is_some() {
                    let contents = opt.unwrap();
                    let old = bincode::deserialize::<BillboardLocation>(contents.as_ref()).unwrap();
                    let old_channel = ChannelId::new(old.channel_id);
                    let edit = old_channel.edit_message(&ctx, old.message_id, EditMessage::new().embed(generate_perfmon_embed(true))).await;

                    if let Err(why) = edit {
                        eprintln!("Error sending message: {why:?}");
                    };
                }
            }
            PerfmonCommand::Die => {
                if opt.is_some() {
                    let contents = opt.unwrap();
                    let old = bincode::deserialize::<BillboardLocation>(contents.as_ref()).unwrap();
                    let old_channel = ChannelId::new(old.channel_id);
                    let edit = old_channel.edit_message(&ctx, old.message_id, EditMessage::new().embed(generate_perfmon_embed(false))).await;

                    if let Err(why) = edit {
                        eprintln!("Error sending message: {why:?}");
                    };
                }
            }
        }
    }
   
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
