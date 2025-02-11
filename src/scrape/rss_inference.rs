//TODO re-add local llm or use bert

use std::time::Duration;
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::time::sleep;
use tracing::{info, instrument, warn};

static PROMPT: &str = r#"
Please read the following document and analyze whether the company plans to execute a reverse stock split. Then, if the company plans to execute a reverse stock split, classify whether the company plans to round up fractional shares in a reverse stock split using one of the following categories: RoundUp, RoundDown, Cash, NotSplit, AlreadyHappened, OTC, Other. If it seems like the company has already split and is just notifying shareholders, use AlreadyHappened. If the stock is not traded on the NYSE or NASDAQ, use OTC. 

Additionally, extract the ex-date (the date the split takes effect) and predict when the stock will reappear on exchanges based on the document's information. Cite your sources in the document in your reasoning.

Ensure your response is a JSON object in the following format (without comments):
{
  "reasoning": "something",
  "ticker": "something", // the company's corresponding NYSE or NASDAQ stock ticker, all caps (4 characters max)
  "classification": "RoundUp",  // only allows (case-sensitive) one of (RoundUp, RoundDown, Cash, NotSplit, OTC, Other)
  "ex_date": "something",  //  ISO 8601 datetime for the ex-date with UTC timezone, or null if not found
}

Document:
{}
"#;

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub enum Classification {
    RoundUp,
    RoundDown,
    Cash,
    NotSplit,
    AlreadyHappened,
    OTC,
    Other
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub struct InferenceOutput {
    pub reasoning: String,
    pub ticker: String,
    pub classification: Classification,
    pub ex_date: Option<DateTime<Utc>>
}

pub struct LLMInference<'a> {
    client: &'a Client,
    api_key: &'a str,
}

impl<'a> LLMInference<'a> {
    pub fn new(client: &'a Client, api_key: &'a str) -> Self {
        Self { client, api_key }
    }
}

pub trait Inference {
    async fn infer(&self, document_text: &Vec<String>) -> anyhow::Result<InferenceOutput>;
}

#[derive(Debug, Error)]
enum InferenceError {

    #[error(transparent)]
    RequestError(#[from] reqwest::Error),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    
    #[error("rate limited")]
    RateLimitedError(StatusCode)
}

impl Inference for LLMInference<'_> {
    
   /* #[instrument(skip_all)]
    async fn infer(&self, document_text: &Vec<String>) -> anyhow::Result<InferenceOutput> {
        info!("Inferencing with LLM model");
        
        let request_body = json!({
        "model": "gpt-4o-mini",
        "messages": [
            { "role": "system", "content": "You are an expert financial analyst." },
            { "role": "user", "content": PROMPT.replace("{}", &document_text.join("\n")) }
        ]
        });
        let response = self.client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request_body)
            .send()
            .await?;
        
        if response.status() != StatusCode::OK {
            return Err(InferenceError::RateLimitedError(response.status()).into());
        }
        
        let response_json: serde_json::Value = response.json().await?;
        let chatgpt_response = response_json["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .to_string()
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .to_string();

        info!("done with inference");
        
        let chatgpt_response = serde_json::from_str::<InferenceOutput>(&chatgpt_response)?;

        Ok(chatgpt_response)
    }*/
    
    #[instrument(skip_all)]
    async fn infer(&self, document_text: &Vec<String>) -> anyhow::Result<InferenceOutput> {
        info!("Inferencing with LLM model");

        let request_body = json!({
        "model": "gpt-4o-mini",
        "messages": [
            { "role": "system", "content": "You are an expert financial analyst." },
            { "role": "user", "content": PROMPT.replace("{}", &document_text.join("\n")) }
        ]
    });

        let mut attempt = 0;
        let max_attempts = 9;
        let mut delay = Duration::from_secs(1);

        while attempt < max_attempts {
            let response = self.client
                .post("https://api.openai.com/v1/chat/completions")
                .header("Authorization", format!("Bearer {}", self.api_key))
                .json(&request_body)
                .send()
                .await?;
            
            info!("request sent and recvs");
            
            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                attempt += 1;
                warn!("Rate limited. Retrying in {:?}...", delay);
                sleep(delay).await;
                delay *= 2;  // Exponential backoff
                continue;
            }

            let response_json: serde_json::Value = response.json().await?;
            let chatgpt_response = response_json["choices"][0]["message"]["content"]
                .as_str()
                .unwrap_or("")
                .to_string()
                .trim()
                .trim_start_matches("json")
                .trim_start_matches("")
                .trim_end_matches("`")
                .to_string();

            info!("done with inference");

            let chatgpt_response = serde_json::from_str::<InferenceOutput>(&chatgpt_response)?;
            return Ok(chatgpt_response);
        }

        Err(anyhow::anyhow!("Exceeded maximum retries due to rate limiting"))
    }
}


mod tests {
    use dotenv::dotenv;
    use reqwest::Client;
    use serde_json::json;
    use crate::scrape::rss_inference::{Classification, Inference, InferenceOutput, LLMInference};

    #[tokio::test]
    async fn test_infer() {
        dotenv().ok();
        let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY must be set");
    
        let client = Client::new();
        let inference = LLMInference::new(&client, &api_key);
    
        let document_text = vec![
            "This is a sample document about AAPL".to_string(),
            "It contains information about a 1 to 5 reverse stock split".to_string(),
        ];
    
        let result = inference.infer(&document_text).await;
    
        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.ticker, "AAPL");
        assert_eq!(output.classification, Classification::RoundUp);
        assert!(output.ex_date.is_none());
    }
    
    #[test]
    fn test_deserialization_with_chatgpt_real_output() {

        let sample_json = json!({
            "reasoning": "The document states that the split will be rounded up and specifies that it is happening on 1/2/2024 (as 'tomorrow' refers to the day after 1/1/2024).",
            "ticker": "AAPL",
            "classification": "RoundUp",
            "ex_date": "2024-01-02T00:00:00Z"
        });

        let deserialized: InferenceOutput = serde_json::from_value(sample_json).expect("Failed to deserialize JSON");

        assert_eq!(deserialized.reasoning, "The document states that the split will be rounded up and specifies that it is happening on 1/2/2024 (as 'tomorrow' refers to the day after 1/1/2024).");
        assert_eq!(deserialized.ticker, "AAPL");
        assert_eq!(deserialized.classification, Classification::RoundUp);
        assert_eq!(
            deserialized.ex_date.unwrap().to_rfc3339(),
            "2024-01-02T00:00:00+00:00"
        );
    }

    #[test]
    fn test_internal_inference_output_deserialization() {
        let sample_json = json!({
            "reasoning": "Based on the company's announcement, they plan to round up fractional shares.",
            "ticker": "AAPL",
            "classification": "RoundUp",
            "ex_date": "2023-12-15T00:00:00Z"
        });

        let deserialized: InferenceOutput = serde_json::from_value(sample_json).expect("Failed to deserialize JSON");

        assert_eq!(deserialized.reasoning, "Based on the company's announcement, they plan to round up fractional shares.");
        assert_eq!(deserialized.ticker, "AAPL");
        assert_eq!(deserialized.classification, Classification::RoundUp);
        assert_eq!(
            deserialized.ex_date.unwrap().to_rfc3339(),
            "2023-12-15T00:00:00+00:00"
        );
    }

    #[tokio::test]
    async fn test_spam() {
        dotenv().ok();
        let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY must be set");

        let client = Client::new();
        let inference = LLMInference::new(&client, &api_key);

        let document_text = vec![
            "This is a sample document about AAPL".to_string(),
            "It contains information about a 1 to 5 reverse stock split".to_string(),
        ];

        let result = inference.infer(&document_text).await;
        println!("1");
        let result = inference.infer(&document_text).await;
        println!("2");
        let result = inference.infer(&document_text).await;
        println!("3");
        let result = inference.infer(&document_text).await;
        println!("4");
        let result = inference.infer(&document_text).await;
        println!("5");
        
        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.ticker, "AAPL");
        assert_eq!(output.classification, Classification::RoundUp);
        assert!(output.ex_date.is_none());
    }
    #[test]
    fn test_internal_inference_output_deserialization_with_null_ex_date() {
        let sample_json = json!({
            "reasoning": "The company did not specify a date for the split.",
            "ticker": "MSFT",
            "classification": "NotSplit",
            "ex_date": null
        });

        let deserialized: InferenceOutput = serde_json::from_value(sample_json).expect("Failed to deserialize JSON");

        assert_eq!(deserialized.reasoning, "The company did not specify a date for the split.");
        assert_eq!(deserialized.ticker, "MSFT");
        assert_eq!(deserialized.classification, Classification::NotSplit);
        assert!(deserialized.ex_date.is_none());
    }
}

