pub enum BuysellCommand {
    Buy(String),
    Sell(String),
    Die
}

trait MarketAccount {
    
    async fn check_ticker_present() -> bool;
    
    async fn buy();
    async fn sell();
}

pub struct BuySellTask {}

impl BuySellTask {
    pub async fn run() {
        
    }
    
    fn buy() {
        
    }
    
    fn sell() {
        
    }
}

struct Robinhood {
    
}

impl MarketAccount for Robinhood {
    async fn check_ticker_present() -> bool {
        todo!()
    }

    async fn buy() {
        todo!()
    }

    async fn sell() {
        todo!()
    }
}