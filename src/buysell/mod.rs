pub mod python;

use apalis::prelude::Data;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{info, instrument};
use crate::buysell::python::PythonAllService;

type Ticker = String;

#[derive(Serialize, Deserialize, Debug)]
pub enum Action {
    Buy,
    Sell
}

#[derive(Debug,Serialize, Deserialize)]
pub struct BuyTask {
    action: Action,
    ticker: String,
    
    
    //message_id: u64, //TODO 
    #[serde(skip)] notify: Option<oneshot::Sender<()>>
}
impl BuyTask {
    pub fn new(action: Action, ticker: String) -> Self {
        Self { action, ticker, notify: None }
    }
    
    pub fn new_notify(action: Action, ticker: String) -> (Self, oneshot::Receiver<()>) {
        let (tx,rx) = oneshot::channel::<()>();

        (Self { action, ticker, notify: Some(tx) }, rx)
    }
    
    pub async fn unreliable_done(self) -> anyhow::Result<()> {
        if let Some(notify) = self.notify {
            let _ = notify.send(());
            
            return Ok(());
        }
        
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum BuyError {
    #[error(transparent)]
    DataError(#[from] anyhow::Error),
}


#[instrument(skip_all)]
pub async fn buy_task(task: BuyTask, svc: Data<PythonAllService>) -> Result<(), BuyError> {
    info!("executing {:#?}", task);
    
    //svc.process(&task).await?;
    task.unreliable_done().await?;

    Ok(())
}

