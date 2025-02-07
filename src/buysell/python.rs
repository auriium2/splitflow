use anyhow::bail;
use reqwest::Client;

use crate::buysell::{Action, BuyTask};
use serde::{Deserialize, Serialize};


#[derive(Serialize)]
struct OrderRequest {
    action: String,
    amount: isize,
    stock: String,
    dry: bool,
}

#[derive(Deserialize)]
struct OrderResponse {
    logs: Vec<String>
}

#[derive(Clone)]
pub struct PythonAllService {
    unproxied_client: Client,
    buyserver_url: String,
}

impl PythonAllService {
    pub fn new(unproxied_client: Client, buyserver_url: String) -> Self {
        Self { unproxied_client, buyserver_url }
    }

    pub(crate) async fn process(&self, task: &BuyTask) -> anyhow::Result<()> {
        let action = match task.action {
            Action::Buy => {"buy"}
            Action::Sell => {"sell"}
        }.to_string();

        let ticker: String = task.ticker.clone();
        let order = OrderRequest { action, amount: 1, stock: ticker, dry: true, };
        let response = self.unproxied_client.post(&*self.buyserver_url)
            .json(&order)
            .send()
            .await?;

        if !response.status().is_success() { bail!("failed to call api, status {}", response.status())}

        //let order_response: OrderResponse =  response.json::<OrderResponse>().await?;


        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use crate::buysell::python::PythonAllService;
    use reqwest::Client;


    use httpmock::Method::POST;
    use httpmock::MockServer;
    use serde_json::json;
    use crate::buysell::{Action, BuyTask};

    #[tokio::test]
    async fn test_process_real_server() {
        // Create a PythonPurchaser instance with the real server URL
        let purchaser = PythonAllService {
            unproxied_client: Client::new(),
            buyserver_url: "http://localhost:8080".to_string(),
        };
        // Create a BuyTask instance
        let task = BuyTask::new(Action::Buy, "AAPL".to_string());
        // Call the process method with the task
        let result = purchaser.process(&task).await;

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
                    "amount": 1,
                    "stock": "AAPL",
                    "dry": true
                }));
            then.status(200);
        });

        // Create a PythonPurchaser instance with the mock server URL
        let purchaser = PythonAllService {
            unproxied_client: Client::new(),
            buyserver_url: server.url("/"),
        };

        // Call the buy method
        let task = BuyTask::new(Action::Buy, "AAPL".to_string());
        // Call the process method with the task
        let result = purchaser.process(&task).await;
        
        println!("{:?}", result);

        // Assert that the result is Ok
        assert!(result.is_ok());

        // Assert that the mock was called exactly once
        _mock.assert();
    }
}


