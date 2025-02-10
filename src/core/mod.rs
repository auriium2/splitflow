use anyhow::Result;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

pub mod database;
pub mod load;
pub mod queue;

// CONFIG STUFF
pub const APP_NAME: &str = "splitflow";
pub const APP_DEV: &str = "auriium softworks";


// PUBLIC TYPES
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SplitflowConfig {
    pub redis_url: String,
    pub mongo_url: String,
    pub proxy_url: String,
    pub buyserver_url: String,
    pub announcement_channel: String,
    pub discord_token: String,
    pub gpt_key: String,
}

pub struct Ticker(pub String);
impl FromStr for Ticker {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ticker_regex = Regex::new(r"^[A-Z]{1,5}$").unwrap();
        if ticker_regex.is_match(s) {
            Ok(Ticker(s.to_uppercase()))
        } else {
            Err("Invalid stock ticker. Must be 1-5 uppercase letters.")
        }
    }
}


pub type UUID = String;
pub type Link = String;
pub type Body = String;

