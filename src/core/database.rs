use crate::core::UUID;
use anyhow::Result;
use bson::{doc, Bson};
use mongodb::Collection;
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Deserialize, Serialize, Debug)]
pub struct CompanyDocument {
    
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct SignpostDocument {
    signpost_id: String,
    pub channel_id: String,
    pub message_id: String
}

impl SignpostDocument {
    pub fn new(signpost_id: String, channel_id: String, message_id: String) -> Self {
        Self { signpost_id, channel_id, message_id }
    }
}

pub struct CoreDB {
    signpost_db: Collection<SignpostDocument>,
    document_db: Collection<CompanyDocument>,
    document_cache: Cache<UUID, Option<CompanyDocument>>
    
    //play collection
    //play cache
}

type OldID = u64;
impl CoreDB {
    
    pub async fn place_or_move_signpost(&self, new_signpost: SignpostDocument) -> Result<Option<SignpostDocument>> {
        let query = doc! { "signpost_id": &new_signpost.signpost_id };
        let old_signpost = self.signpost_db.find_one(query.clone()).await?;
        if old_signpost.is_some() {
            let update = doc! { "$set": doc! {"message_id": new_signpost.message_id} };
            self.signpost_db.update_one(query, update).await?;
            
            Ok(old_signpost)
        } else {
            self.signpost_db.insert_one(SignpostDocument {
                ..new_signpost
            }).await?;
            
            Ok(None)
        }
        
    }

    
    pub async fn is_present(&self, uuid: &UUID) -> Result<bool> {
        let cache_option = self.document_cache.get(uuid);
        if cache_option.is_some() {
            Ok(true)
        } else {
            //let's ask the database then?
            
            let database_option: Option<CompanyDocument> = self.document_db.find_one(doc! {
                "uuid": uuid
            }).await?;
            
           if database_option.is_none() {
               Ok(false) //i could write to the cache that theres no document but theres really no point
           } else {
               self.document_cache.insert(uuid.clone(), database_option);                //update the cache
               Ok(true)
           }
        }
        
    }
}


