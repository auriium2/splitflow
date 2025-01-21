mod robinhood;

use serde::{Deserialize, Serialize};

type Ticker = String;

#[derive(Serialize, Deserialize)]
pub enum BuySellTask {
    Buy(Ticker),
    Sell(Ticker),
}

//TODO persistence?

pub enum BuysellCommand {
    Buy(Ticker),
    Sell(Ticker),
    Die,
}

trait MarketAccount {
    async fn check_ticker_present() -> bool;

    async fn buy(ticker: &str);
    async fn sell(ticker: &str);
}

impl BuySellTask {
    pub async fn run() {}

    fn buy() {}

    fn sell() {}
}
