//TODO re-add local llm or use bert

use reqwest::Client;
use serde::Deserialize;
use serde_json::json;


static PROMPT: &str = r#"
Please read the following document and analyze whether the company plans to round up fractional shares in a reverse stock split. Then, classify the plan using one of the following categories: ROUND_UP, ROUND_DOWN, CASH, NOT_SPLIT, OTHER. 

Additionally, extract the ex-date (the date the split takes effect) and predict when the stock will reappear on exchanges based on the document's information.

Ensure your response is a JSON object in the following format (without comments):
{
  "reasoning": "something",
  "classification": "ROUND_UP",  // or one of ROUND_DOWN, CASH, NOT_SPLIT, OTHER
  "ex_date": "something",  // UTC datetime for the ex-date, or null if not found
  "predicted_date": "something"  // UTC datetime for stock reappearance, or null if not found
}

Document:
{}
"#;

#[derive(Debug, Deserialize)]
struct AIOutput {
    reasoning: String,
    classification: String,
    ex_date: Option<String>,       // Use Option for nullable fields
    predicted_date: Option<String> // Use Option for nullable fields
}

struct LLMInference {
    api_key: String,
}

//lol lmao
pub async fn gpt_infer(api_key: &str, document_text: &str, client: &Client) -> anyhow::Result<String> {
    
    let request_body = json!({
        "model": "gpt-4o-mini",
        "messages": [
            { "role": "system", "content": "You are an expert financial analyst." },
            { "role": "user", "content": PROMPT.replace("{}", document_text) }
        ]
    });
    let response = client
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
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
    
    Ok(chatgpt_response)
}




