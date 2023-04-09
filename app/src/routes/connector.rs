use crate::AppState;
use actix_web::{get, post, web, HttpResponse};
use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, Serialize, Debug)]
struct GetResponse {
    connector_names: Vec<String>,
}

#[get("/connectors")]
pub async fn get_connectors(state: web::Data<AppState>) -> HttpResponse {
    let active_connectors = &state.active_connectors;

    let connector_names: Vec<String>;
    {
        connector_names = active_connectors
            .lock()
            .unwrap()
            .keys()
            .map(|s| s.to_string())
            .collect();
    }

    let resp = GetResponse {
        connector_names
    };
    HttpResponse::Ok().json(resp)
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PostBody {
    pub name: String,
    pub config: serde_json::Value,
}

#[post("/connectors")]
pub async fn post_connectors(
    body: web::Json<PostBody>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let kafka = &state.kafka;

    let topic = &kafka.config.config_topic;
    let key = body.name.as_bytes();
    let message = body.config.to_string();
    let message = message.as_bytes();

    let record = FutureRecord::to(topic).key(key).payload(message);
    let status = kafka.producer.send(record, Duration::from_secs(0)).await;

    match status {
        Ok(r) => {
            println!("Message sent: {:?}", r);
            HttpResponse::Ok().json(body)
        }
        Err(e) => {
            println!("Error sending message: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}
