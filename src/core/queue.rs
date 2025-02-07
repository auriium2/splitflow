use crate::buysell::BuyTask;
use crate::discord2::announce::DiscordTask;
use apalis::prelude::{MemoryStorage, MessageQueue, Storage};
use apalis_redis::RedisStorage;
use tokio::sync::Mutex;
use crate::scrape::RSSTask;

pub struct QueueManager {
    buy_queue: Mutex<RedisStorage<BuyTask>>,
    discord_queue: Mutex<MemoryStorage<DiscordTask>>,
    scan_queue: Mutex<MemoryStorage<RSSTask>>
}

impl QueueManager {
    pub async fn push_buy(&self, task: BuyTask) -> anyhow::Result<()> {
        self.buy_queue.lock().await.push(task).await?;
        
        Ok(())
    }
    pub async fn push_discord(&self, task: DiscordTask) -> anyhow::Result<()> {
        self.discord_queue.lock().await.enqueue(task).await.expect("bizarre memorystorage enqueing error!");

        Ok(())
    }

    pub async fn push_scan(&self, task: RSSTask) -> anyhow::Result<()> {
        self.scan_queue.lock().await.enqueue(task).await.expect("bizarre memorystorage enqueing error!");

        Ok(())
    }

    pub fn new(buy_queue: RedisStorage<BuyTask>, discord_queue: MemoryStorage<DiscordTask>, scan_queue: MemoryStorage<RSSTask>) -> Self {
        Self { buy_queue: Mutex::new(buy_queue), discord_queue: Mutex::new(discord_queue), scan_queue: Mutex::new(scan_queue) }
    }
}

