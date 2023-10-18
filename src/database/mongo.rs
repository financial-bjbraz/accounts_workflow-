// use chrono::{TimeZone, Utc};
// use mongodb::bson::{self, doc, Bson};
// use std::env;
// use std::error::Error;
// use mongodb::options::ResolverConfig;
use mongodb::options::ClientOptions;
use mongodb::Client;
// use serde::{Deserialize, Serialize};
// use serde_json::{Result, Value};
use crate::account::Message;

extern crate mongodb;

// #[path = "../service/account.rs"]
// mod account;

    pub async fn insert_on_database(account_object: Message) -> mongodb::error::Result<()> {

        use mongodb::bson::{doc, Document};

        let client_options = ClientOptions::parse(
            "mongodb://192.168.0.18:49155",
        )
        .await?;
        let client = Client::with_options(client_options)?;
        let database = client.database("bank-transactions");
        // do something with database

        let collection = database.collection("account");
        collection.insert_one(account_object, None).await?;

        Ok(())
    }
    // CONNECTION EXAMPLE ENDS HERE
