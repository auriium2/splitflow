use apalis::prelude::Data;
use std::fmt::Display;
use std::fmt;
use std::sync::Arc;
use serenity::all::{ChannelId, Colour, CreateEmbed, CreateEmbedFooter, CreateMessage, Http};
use chrono::Utc;
use tracing::trace;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Deserialize, Serialize, PartialEq, Eq)]
pub enum Where {
    Announcements
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Debug)]
pub enum Source {
    Scanner,
}
impl Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Source::Scanner => "SCANNER",
        };
        write!(f, "{}", s)
    }
}

#[derive(Deserialize, Serialize)]
pub struct AnnounceTask {
    source: Source,
    location: Where,
    string: String,
    color: Colour
}
impl AnnounceTask {
    pub fn new(source: Source, location: Where, string: String, color: Colour) -> Self {
        Self { source, location, string, color }
    }
}

#[derive(Clone)]
pub struct DiscordService {
    discord_client: Arc<Http>,
    announcements: ChannelId
}

impl DiscordService {
    pub fn new(discord_client: Arc<Http>, announcements_channel: &str) -> Self {
        Self { discord_client, announcements: ChannelId::new(announcements_channel.parse().unwrap()) }
    }
    
    #[tracing::instrument(skip_all)]
    pub async fn process(&self, task: AnnounceTask) -> anyhow::Result<()> {
        
        if task.location == Where::Announcements {
            let task_source = task.source.to_string().to_uppercase();
            let embed = CreateEmbed::new()
                .title(format!("splitflow | {}", task_source))
                .color(task.color)
                .description(task.string)
                .footer(CreateEmbedFooter::new("auriium software"))
                .timestamp(Utc::now());

            let message = CreateMessage::new().add_embed(embed);
            self.announcements.send_message(&self.discord_client, message).await?;
        } else {
            trace!("no location")
        }
        
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum TransparentError {
    #[error(transparent)]
    GenericError(#[from] anyhow::Error)
}

pub async fn process_task(task: AnnounceTask, svc: Data<DiscordService>) -> Result<(),TransparentError> {
    svc.process(task).await?;
    
    Ok(())
}