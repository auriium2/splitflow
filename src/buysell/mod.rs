use std::sync::mpsc::Receiver;

pub enum BuysellCommand {
    Buy(String),
    Sell(String),
    Die
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