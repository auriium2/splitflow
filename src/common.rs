use std::fmt::Debug;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use sled::{IVec, Tree};
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

fn guh(t: Tree) {
    t.insert()
}
const j: &[u8; 2] = b"hi";

trait ConvenientDb {
    fn get_or_insert_default<'a, T, const N: usize>(&self, key: &[u8; N], default: &T) -> T where 
        T: Deserialize<'a> + Serialize;
}

impl ConvenientDb for Tree {
    fn get_or_insert_default<'a, T, const N: usize>(&self, key: &[u8; N], default: &T) -> T where
        T: Deserialize<'a> + Serialize,
    
    {
        
        self.insert(j, b"hi");
        
        let o = self.transaction(|tx| {
            let v = tx.get(key)?;
            
            match v {
                None => {
                    let as_vec: IVec = IVec::from(bincode::serialize(default).unwrap());
                    let g = tx.insert(key, as_vec);
                    
                    Ok(())
                    
                }
                Some(item) => {
                    let old: T = bincode::deserialize::<T>(item.as_ref()).unwrap();

                    Ok(())
                }
            }
        })
        
        
    }
}