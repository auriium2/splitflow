//TODO re-add local llm or use bert

use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tracing::instrument;

static PROMPT: &str = r#"
Please read the following document and analyze whether the company plans to round up fractional shares in a reverse stock split. Then, classify the plan using one of the following categories: RoundUp, ROUND_DOWN, CASH, NOT_SPLIT, OTHER. 

Additionally, extract the ex-date (the date the split takes effect) and predict when the stock will reappear on exchanges based on the document's information.

Ensure your response is a JSON object in the following format (without comments):
{
  "reasoning": "something",
  "ticker": "something", // the company's corresponding stock ticker
  "classification": "RoundUp",  // or one of RoundDown, Cash, Other
  "ex_date": "something",  //  ISO 8601 datetime for the ex-date with UTC timezone, or null if not found
}

Document:
{}
"#;

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum Classification {
    RoundUp,
    RoundDown,
    Cash,
    Other,
}

#[derive(Debug, Deserialize)]
pub struct InternalInferenceOutput {
    pub reasoning: String,
    pub ticker: String,
    pub classification: Classification,
    pub ex_date: Option<String>   
}

pub struct ReadableInferenceOutput {
    
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
    async fn infer(&self, document_text: &str) -> anyhow::Result<InternalInferenceOutput>;
}

impl Inference for LLMInference<'_> {
    
    #[instrument(skip(self, document_text), fields(document_text = document_text.len()))]
    async fn infer(&self, document_text: &str) -> anyhow::Result<InternalInferenceOutput> {
        let request_body = json!({
        "model": "gpt-4o-mini",
        "messages": [
            { "role": "system", "content": "You are a expert financial analyst" },
            { "role": "user", "content": PROMPT.replace("{}", document_text) }
        ]
    });
        let response = self.client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request_body)
            .send()
            .await?;
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

        let chatgpt_response = serde_json::from_str::<InternalInferenceOutput>(&chatgpt_response)?;

        Ok(chatgpt_response)
    }
}




