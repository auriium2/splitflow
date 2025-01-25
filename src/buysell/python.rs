use crate::buysell::Purchaser;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

struct PythonPurchaser {
    client: Client
}

#[async_trait]
impl Purchaser for PythonPurchaser {
    async fn check_ticker_present(&self) -> bool {
        todo!()
    }

    async fn buy(&self, ticker: &str) -> anyhow::Result<()> {
        self.client
            .post("http://localhost:8080/")
            .json(&json!({ "ticker": ticker }))
            .send()
            .await?;



        todo!()
    }
}

