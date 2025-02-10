use chrono::Utc;
use poise::CreateReply;
use serenity::all::Colour;
use serenity::builder::{CreateEmbed, CreateEmbedFooter};
use crate::core::{APP_DEV, APP_NAME};

pub fn command_bad(message: &str) -> CreateReply {
    
    let embed = CreateEmbed::new()
        .title(format!("{}", APP_NAME))
        .footer(CreateEmbedFooter::new(APP_DEV))
        .color(Colour::RED)
        .timestamp(Utc::now())
        .description(message);
    
    CreateReply::default()
        .reply(true)
        .ephemeral(true)
        .embed(embed)
}

pub fn command_mid(message: &str) -> CreateReply {

    let embed = CreateEmbed::new()
        .title(format!("{}", APP_NAME))
        .footer(CreateEmbedFooter::new(APP_DEV))
        .color(Colour::ORANGE)
        .timestamp(Utc::now())
        .description(message);

    CreateReply::default()
        .reply(true)
        .ephemeral(true)
        .embed(embed)
}
pub fn command_neutral(message: &str) -> CreateReply {

    let embed = CreateEmbed::new()
        .title(format!("{}", APP_NAME))
        .color(Colour::LIGHT_GREY)
        .footer(CreateEmbedFooter::new(APP_DEV))
        .timestamp(Utc::now())
        .description(message);

    CreateReply::default()
        .reply(true)
        .ephemeral(true)
        .embed(embed)
}

pub fn command_good(message: &str) -> CreateReply {

    let embed = CreateEmbed::new()
        .title(format!("{}", APP_NAME))
        .color(Colour::from_rgb(144, 238, 144))
        .footer(CreateEmbedFooter::new(APP_DEV))
        .timestamp(Utc::now())
        .description(message);

    CreateReply::default()
        .reply(true)
        .ephemeral(true)
        .embed(embed)
}