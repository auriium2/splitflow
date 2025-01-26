mod python;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

type Ticker = String;

#[derive(Serialize, Deserialize)]
pub enum Action {
    Buy,
    Sell
}

#[derive(Serialize, Deserialize)]
pub struct BuyTask {
    action: Action,
    ticker: String
}

impl BuyTask {
    pub fn new(action: Action, ticker: String) -> Self {
        Self { action, ticker }
    }
}

pub struct BuyService {
    purchasers: Vec<Box<dyn Purchaser>>
    
    //TODO company mongo storage
}

#[derive(Debug, Error)]
enum BuyServiceError {
    
    #[error(transparent)]
    GenericError(#[from] anyhow::Error)
}

#[async_trait]
trait Purchaser {
    async fn buy(&self, ticker: &str) -> anyhow::Result<()>;
}

#[async_trait]
trait Seller {
    async fn buy(&self, ticker: &str) -> anyhow::Result<()>;
}


impl BuyService {
    async fn buy(&self, ticker: &str) -> Result<(),BuyServiceError> {
        for x in &self.purchasers {
            x.buy(ticker).await?;
        }
        
        Ok(())
    }
}

