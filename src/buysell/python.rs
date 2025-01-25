use crate::buysell::Purchaser;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

use serde::Serialize;
use std::collections::HashMap;


struct PythonPurchaser {
    free_client: Client
}

#[derive(Serialize)]
struct OrderRequest {
    action: String,
    amount: f64,
    stock: String,
    dry: bool,
}

#[async_trait]
impl Purchaser for PythonPurchaser {
    async fn check_ticker_present(&self) -> bool {
        todo!()
    }

    async fn buy(&self, ticker: &str) -> anyhow::Result<()> {
        let url = "http://localhost:8080"; // Replace with your actual API URL

        let order = OrderRequest {
            action: "buy".to_string(),
            amount: 10.0,
            stock: ticker.to_string(),
            dry: true,
        };

        let response = self.free_client.post(url)
            .json(&order)
            .send()
            .await?;

        if response.status().is_success() {
            let text = response.text().await?;
            Ok(())
        } else {
            anyhow::bail!("Failed to call API: {}", response.status());
        }
    }
    
    
    
}


#[cfg(test)]
mod tests {
    use reqwest::Client;
    use crate::buysell::Purchaser;
    use crate::buysell::python::PythonPurchaser;

    use super::*;
    use httpmock::MockServer;
    use httpmock::Method::POST;
    use serde_json::json;
  
    #[tokio::test]
    async fn test_buy_real_server() {
        // Create a PythonPurchaser instance with the real server URL
        let purchaser = PythonPurchaser {
            free_client: Client::new(),
        };

        // Call the buy method with a real ticker
        let result = purchaser.buy("AAPL").await;

        println!("{:?}", result);
        // Assert that the result is Ok
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_buy() {
        // Start a local mock server
        let server = MockServer::start();

        // Create a mock for the buy endpoint
        let _mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .json_body(json!({
                    "action": "buy",
                    "amount": 10.0,
                    "stock": "AAPL",
                    "dry": true
                }));
            then.status(200);
        });

        // Create a PythonPurchaser instance with the mock server URL
        let purchaser = PythonPurchaser {
            free_client: Client::new(),
        };

        // Call the buy method
        let result = purchaser.buy("AAPL").await;

        // Assert that the result is Ok
        assert!(result.is_ok());

        // Assert that the mock was called exactly once
        _mock.assert();
    }
}


