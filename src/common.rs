use std::fmt::Debug;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use sled::{IVec, Tree};
use sled::transaction::{ConflictableTransactionResult, TransactionResult, UnabortableTransactionError};
use crate::billboard::BillboardLocation;

use crate::bootstrap::Core;

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
pub type DynResult<T> = Result<T, DynError>;
pub type DynNothing = DynResult<()>;
pub type PoiseContext<'a> = poise::Context<'a, Arc<Core>, DynError>;

const AURIIUM_NAME: &str = "auriium's software";

pub enum Command {
    Tick,
    Stop
}
