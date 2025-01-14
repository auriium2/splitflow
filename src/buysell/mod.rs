use std::sync::mpsc::Receiver;

pub enum BuysellCommand {
    Buy(String),
    Sell(String),
    Die
}

trait MarketAccount {
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

//this task should be run every weekday at market open, and rapidly buy/sell all the flagged stocks if they are bad/good
pub async fn task_buysell(rx: Receiver<BuysellCommand>) {
    
}

async fn buy() -> anyhow::Result<()> {
    todo!();
}

async fn sell() -> anyhow::Result<()> {
    todo!();
}