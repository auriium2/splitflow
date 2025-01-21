mod robinhood;

use serde::{Deserialize, Serialize};

type Ticker = String;

#[derive(Serialize, Deserialize)]
pub struct BuyTask {
    
}



trait MarketAccount {
    async fn check_ticker_present() -> bool;

    async fn buy(ticker: &str);
    async fn sell(ticker: &str);
}

impl BuyTask {
    pub async fn run() {}

    fn buy() {}

    fn sell() {}
}
