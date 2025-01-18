mod robinhood;

use tokio::sync::mpsc::Receiver;

type Ticker = String;

pub enum BuysellCommand {
    Buy(Ticker),
    Sell(Ticker),
    Die
}

trait MarketAccount {
    
    async fn check_ticker_present() -> bool;
    
    async fn buy(ticker: &str);
    async fn sell(ticker: &str);
}

pub struct BuySellTask {
    rx: Receiver<BuysellCommand>
}

impl BuySellTask {
    pub async fn run() {
        
    }
    
    fn buy() {
        
    }
    
    fn sell() {
        
    }
}

