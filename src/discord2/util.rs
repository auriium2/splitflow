use chrono::Utc;
use poise::CreateReply;
use serenity::all::Colour;
use serenity::builder::CreateEmbed;

pub fn command_bad(message: &str) -> CreateReply {
    
    let embed = CreateEmbed::new()
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
        .color(Colour::LIGHT_GREY)
        .timestamp(Utc::now())
        .description(message);

    CreateReply::default()
        .reply(true)
        .ephemeral(true)
        .embed(embed)
}

pub fn command_good(message: &str) -> CreateReply {

    let embed = CreateEmbed::new()
        .color(Colour::from_rgb(144, 238, 144))
        .timestamp(Utc::now())
        .description(message);

    CreateReply::default()
        .reply(true)
        .ephemeral(true)
        .embed(embed)
}