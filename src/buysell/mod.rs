mod python;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

type Ticker = String;

#[derive(Serialize, Deserialize)]
pub struct BuyTask {
    ticker: String
}

pub struct BuySellService {
    purchasers: Vec<Box<dyn Purchaser>>
}

#[derive(Debug, Error)]
enum BuyServiceError {
    
    #[error(transparent)]
    GenericError(#[from] anyhow::Error)
}

#[async_trait]
trait Purchaser {
    async fn check_ticker_present(&self) -> bool;

    async fn buy(&self, ticker: &str) -> anyhow::Result<()>;
}

impl BuySellService {
    async fn buy(&self, ticker: &str) -> Result<(),BuyServiceError> {
        for x in &self.purchasers {
            x.buy(ticker).await?;
        }
        
        Ok(())
    }
}

