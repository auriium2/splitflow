use crate::billboard::PERFMON_BB;
use crate::core::Core;
use apalis::prelude::{Context, Data, Worker};
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serenity::all::{ChannelId, Colour, CreateEmbed, CreateEmbedFooter, EditMessage, Http, Timestamp};
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, trace};

#[derive(Error, Debug)]
pub enum PerfmonError {
    #[error(transparent)]
    ParseError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    EditError(#[from] serenity::Error),

    #[error(transparent)]
    HttpError(#[from] reqwest::Error),

    #[error(transparent)]
    DataError(#[from] anyhow::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PerfmonTask {}

impl From<DateTime<Utc>> for PerfmonTask {
    fn from(_value: DateTime<Utc>) -> Self {
        PerfmonTask {}
    }
}

pub async fn run_perfmon(_task: PerfmonTask, core: Data<Arc<Core>>, discord: Data<Arc<Http>>, worker: Worker<Context>) -> Result<(), PerfmonError> {
    //info!("is_shutting_down: {}", worker.is_shutting_down());
    
    trace!("running perfmon task");
    let opt = core.db.get_signpost(PERFMON_BB.to_string()).await?;
    
    if let Some(contents) = opt {
        let old_channel = ChannelId::new(contents.channel_id.parse::<u64>()?);

        let embed = running_embed();
        let message_id = contents.message_id.parse::<u64>()?;
    
        if worker.is_shutting_down() {
            let embed = offline_embed();
            
            old_channel
                .edit_message(&*discord, message_id, EditMessage::new().embed(embed))
                .await?;
            return Ok(());
        }
        
        old_channel
            .edit_message(&*discord, message_id, EditMessage::new().embed(embed))
            .await?;
    }
    
    Ok(())
   
}




fn offline_embed() -> CreateEmbed {
    let c: Colour = Colour::from_rgb(255,120,120);
    let embed = CreateEmbed::new()
        .color(c)
        .title("splitflow | perfmon")
        .description("The bot is currently offline. Please check back later.")
        .footer(CreateEmbedFooter::new("auriium software"))
        .timestamp(Timestamp::now());
    
    embed

}

fn running_embed() -> CreateEmbed {
    let cpu_load = sys_info::loadavg().unwrap();
    let mem_use = sys_info::mem_info().unwrap();

    let c: Colour = Colour::from_rgb(120,255,120);
    let embed = CreateEmbed::new()
        .color(c)
        .title("splitflow | perfmon")
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
