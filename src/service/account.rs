
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};


#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub idempotency_key: String,
    pub account_number: i32,
    pub owner: Person,
    pub responsible: Person,
    pub account_status: Option<String>
}

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
#[serde(rename_all = "camelCase")]
pub struct Address {
    street: Option<String>,
    number: Option<String>,
    complement: Option<String>,
    district: Option<String>,
    zipcode: Option<String>,
    state: Option<String>,
    city: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
#[serde(rename_all = "camelCase")]
pub struct Document {
    document: Option<String>,
    document_type: Option<String>
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone)]
pub struct Person {
    name: String,
    person_type: Option<String>,
    birth_date: String,
    company_name: Option<String>,
    email: Option<String>,
    home_phone: Option<String>,
    business_phone: Option<String>,
    mobile_phone: String,
    address: Option<Address>,
    identifier_document: Document,
    documents: Vec<Document>,
}

pub fn convert_to_object(adata: String) -> Result<Message> { //-> Result<(Message, serde_json::Error)>
    // Some JSON input data as a &str. Maybe this comes from the user.
    
    // Parse the string of data into a Person object. This is exactly the
    // same function as the one that produced serde_json::Value above, but
    // now we are asking it for a Person as output.
    let p: Message = serde_json::from_str(&adata).unwrap();

    // Do things just like with any other Rust data structure.
    println!("Please call {} at the number {}", p.owner.name, p.responsible.name);

    Ok(p)
}

pub fn deserializer(message:Message) -> Result<String> {
    let p : String = serde_json::to_string(&message).unwrap();
    Ok(p)
}