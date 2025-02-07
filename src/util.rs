
use apalis::prelude::{Backend, Context, MemoryStorage, Poller, Request, RequestStream, Worker};
use apalis_cron::CronStream;
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt};
use tower::layer::util::Identity;

pub struct MergedStorage<T> {
    memory_storage: MemoryStorage<T>,
    cron_stream: CronStream<T, Utc>,
    
}

impl<T> MergedStorage<T> {
    pub fn new(memory_storage: MemoryStorage<T>, cron_stream: CronStream<T, Utc>) -> Self {
        Self { memory_storage, cron_stream }
    }
}

impl<T> Backend<Request<T, ()>, ()> for MergedStorage<T>
where
    T: From<DateTime<Utc>> + Send + Sync + 'static,
{
    type Stream = RequestStream<Request<T, ()>>;
    type Layer = Identity;

    fn poll<Svc: tower::Service<Request<T, ()>>>(self, worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let memory_stream = self
            .memory_storage
            .poll::<Svc>(worker)
            .stream
            .boxed();

        let cron_stream = self
            .cron_stream
            .poll::<Svc>(worker)
            .stream
            .boxed();

        // Merge the two streams and box the result.
        let merged_stream = futures::stream::select(memory_stream, cron_stream).boxed();

        Poller::new(merged_stream, futures::future::pending())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use apalis::prelude::*;
    use apalis_cron::Schedule;
    use chrono::Utc;
    use futures::StreamExt;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicIsize};
    use serde::{Deserialize, Serialize};
    use tower::limit::ConcurrencyLimitLayer;
    use tower::load_shed::LoadShedLayer;
    use crate::scrape::rss_task;
    use thiserror::Error;
    use tokio::sync::Mutex;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestTask {
        i: u8
    }
    impl From<DateTime<Utc>> for TestTask {
        fn from(value: DateTime<Utc>) -> Self {
            Self{ i: 1 }
        }
    }

    #[derive(Error, Debug)]
    enum DebugError {
        #[error(transparent)]
        GenericError(#[from] anyhow::Error)
    }
    
    pub async fn run_task(task: TestTask, counter: Data<Arc<Mutex<bool>>>) -> Result<(),DebugError> {
        if task.i == 0 {
            *counter.lock().await = true;
        }
        
        
        Ok(())
    }


    #[tokio::test]
    async fn test_merged_storage_poll() -> anyhow::Result<()> {
        let memory_storage = MemoryStorage::<TestTask>::new();
        let cron_schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let cron_stream = CronStream::new(cron_schedule);
        let mut memory_copy = memory_storage.clone();

        let merged_storage = MergedStorage {
            memory_storage,
            cron_stream,
        };
        
        let data = Arc::new(Mutex::new(false));
        let dat_clone = data.clone();

        let rss_worker = WorkerBuilder::new("scraper")
            .enable_tracing()
            .layer(LoadShedLayer::new())
            .layer(ConcurrencyLimitLayer::new(1))
            .data(data)
            .backend(merged_storage)
            .build_fn(run_task);
        
        
        memory_copy.enqueue(TestTask { i: 0 }).await.expect("what");
        tokio::time::timeout(std::time::Duration::from_secs(30), Monitor::new().register(rss_worker).run()).await??;
        
        assert!(*dat_clone.lock().await);
        
        Ok(())
            
    }
}