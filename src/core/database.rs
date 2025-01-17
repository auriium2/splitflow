use crate::core::UUID;
use crate::scrape::rss_inference::InferenceOutput;
use crate::scrape::rss_presence::RssPresence;
use anyhow::Result;
use bson::{doc, DateTime};
use mongodb::Collection;
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;

#[derive(Clone, Deserialize, Serialize, Debug, Hash, PartialEq, Eq)]
pub struct FilingDocument {
    
    //id, possible company
    pub uuid: String,
    pub published: DateTime,
    
    //metrics
    pub is_split: RssPresence,

    //post inference
    pub post_inference: Option<InferenceOutput>,

    //mass data
    pub body_contents: String,
}

impl FilingDocument {
    pub fn new(uuid: String, published: DateTime, is_split: RssPresence, post_inference: Option<InferenceOutput>, body_contents: String) -> Self {
        Self { uuid, published, is_split, post_inference, body_contents }
    }
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
    filing_cache: Cache<UUID, Option<Arc<FilingDocument>>>
    
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
    
    #[instrument(skip_all)]
    pub async fn push_filing_documents(&self, filings: Vec<FilingDocument>) -> Result<()> {
        if filings.is_empty() {
            return Ok(());
        }

        // Batch insert documents into MongoDB
        let documents_to_insert: Vec<_> = filings.iter().cloned().collect();
        self.filing_db.insert_many(documents_to_insert).await?;

        // Update the cache in batches
        for filing in filings {
            let doc_uuid = filing.uuid.clone();
            let reference = Arc::new(filing);
            self.filing_cache.insert(doc_uuid, Some(reference));
        }

        Ok(())
    }
    

    pub async fn get_filing_document(&self, uuid: &UUID) -> Result<Option<Arc<FilingDocument>>> {
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
                let reference = Arc::new(database_option.unwrap());
                let returned_reference = Arc::clone(&reference);
                
                self.filing_cache.insert(uuid.clone(), Some(reference));                //update the cache
                Ok(Some(returned_reference))
            }
        }
    }

    pub fn new(signpost_db: Collection<SignpostDocument>, filing_db: Collection<FilingDocument>, filing_cache: Cache<UUID, Option<Arc<FilingDocument>>>) -> Self {
        Self { signpost_db, filing_db, filing_cache }
    }
}


