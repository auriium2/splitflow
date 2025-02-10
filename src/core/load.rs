use tracing::{info, instrument};
use reqwest::Proxy;
use std::path::Path;
use config::Config;
use tokio::fs;
use crate::core::SplitflowConfig;

#[instrument]
pub async fn load_cfg() -> anyhow::Result<SplitflowConfig> {
    let path = dotenv::dotenv().ok();
    if let Some(path) = path {
        info!("The configuration file path is: {:#?}", fs::canonicalize(path).await?);
    } else {
        info!("No .env file found, loading from environment variables");
    }

    let cfg = Config::builder()
        .add_source(config::Environment::default())
        .build()?
        .try_deserialize::<SplitflowConfig>()?;
    info!("Loaded config");
    
    Ok(cfg)
}

#[instrument]
pub async fn load_proxied_client(cfg: &SplitflowConfig) -> anyhow::Result<reqwest::Client> {
    let proxy = Proxy::all(&cfg.proxy_url)?; //TODO: we should evaluate if we really need to put *every* request through the proxy

    let client = reqwest::Client::builder()
        .gzip(true)
        .brotli(true)
        .zstd(true)
        .proxy(proxy)
        .build()?;
    
    Ok(client)
}

#[instrument]
pub async fn load_unproxied_client(cfg: &SplitflowConfig) -> anyhow::Result<reqwest::Client> {
    let client = reqwest::Client::builder()
        .build()?;

    Ok(client)
}

#[cfg(test)]
mod tests {
    use reqwest::Proxy;
    use crate::core::*;
    #[tokio::test]
    async fn test_live_load_data() -> Result<()> {
        let proxy_url = "http://whokwhmg-rotate:jmditeewz262@p.webshare.io:80/";

        let client = reqwest::Client::builder()
            .proxy(Proxy::all(proxy_url)?)
            .gzip(true)  // Enables automatic decompression
            .build()?;

        let response = client
            .get("https://ipv4.webshare.io/")
            .send()
            .await?;

        // Print the response body
        println!("Response: {}", response.text().await?);

        Ok(())
    }
}