use crate::core::UUID;
use anyhow::Result;
use bson::doc;
use mongodb::Collection;
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Deserialize, Serialize, Debug, Hash, PartialEq, Eq)]
pub struct FilingDocument {
    parsed: bool
}

#[derive(Clone, Deserialize, Serialize, Debug, Hash, PartialEq, Eq)]
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
    filing_db: Collection<FilingDocument>,
    filing_cache: Cache<UUID, Option<FilingDocument>>
    
    //play collection
    //play cache
}

type OldID = u64;
impl CoreDB {
    
    pub async fn get_signpost(&self, signpost_id: String) -> Result<Option<SignpostDocument>> {
        let query = doc! { "signpost_id": signpost_id };
        let signpost = self.signpost_db.find_one(query.clone()).await?;
        Ok(signpost)
    }
    
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
    
    pub async fn get_filing_document(&self, uuid: &UUID) -> Result<Option<FilingDocument>> {
        let cache_option = self.filing_cache.get(uuid);
        if cache_option.is_some() {
            Ok(cache_option.unwrap())
        } else {
            //let's ask the database then?

            let database_option: Option<FilingDocument> = self.filing_db.find_one(doc! {
                "uuid": uuid
            }).await?;

            if database_option.is_none() {
                Ok(None) //i could write to the cache that theres no document but theres really no point
            } else {
                self.filing_cache.insert(uuid.clone(), database_option);                //update the cache
                Ok(database_option)
            }
        }
    }

    pub fn new(signpost_db: Collection<SignpostDocument>, filing_db: Collection<FilingDocument>, filing_cache: Cache<UUID, Option<FilingDocument>>) -> Self {
        Self { signpost_db, filing_db, filing_cache }
    }
}


