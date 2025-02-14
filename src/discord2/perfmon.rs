use serenity::all::{ChannelId, Colour, CreateEmbed, CreateEmbedFooter, EditMessage, Http, Timestamp};
use serde::{Deserialize, Serialize};
use apalis::prelude::{Context, Data, Worker};
use tracing::{instrument, trace};
use thiserror::Error;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use crate::discord2::PERFMON_BB;
use crate::core::database::CoreDB;

#[derive(Debug, Serialize, Deserialize)]
pub struct PerfmonTask {}

#[derive(Clone)]
pub struct PerfmonService{
    db: Arc<CoreDB>,
    discord: Arc<Http>
}
impl From<DateTime<Utc>> for PerfmonTask {
    fn from(_value: DateTime<Utc>) -> Self {
        PerfmonTask {}
    }
}
impl PerfmonService {
    pub fn new(db: Arc<CoreDB>, discord: Arc<Http>) -> Self {
        Self { db, discord }
    }
}

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


#[instrument(skip_all)]
pub async fn perfmon_task(_task: PerfmonTask, svc: Data<PerfmonService>, worker: Worker<Context>) -> Result<(), PerfmonError> {
    //info!("is_shutting_down: {}", worker.is_shutting_down());
    
    let opt = svc.db.get_signpost(PERFMON_BB.to_string()).await?;
    
    if let Some(contents) = opt {
        let old_channel = ChannelId::new(contents.channel_id.parse::<u64>()?);

        let embed = running_embed();
        let message_id = contents.message_id.parse::<u64>()?;
    
        if worker.is_shutting_down() {
            let embed = offline_embed();
            
            old_channel
                .edit_message(&*svc.discord, message_id, EditMessage::new().embed(embed))
                .await?;
            return Ok(());
        }
        
        old_channel
            .edit_message(&*svc.discord, message_id, EditMessage::new().embed(embed))
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