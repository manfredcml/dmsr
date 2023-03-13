use crate::AppState;
use actix_web::{get, post, web, HttpResponse};
use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Deserialize, Serialize, Debug)]
struct GetResponse {
    version: String,
    status: String,
}

#[get("/connectors")]
pub async fn get_connectors(state: web::Data<AppState>) -> HttpResponse {
    let config = &state.kafka.config;
    println!("config: {:?}", config);

    let resp = GetResponse {
        version: "0.0.1".to_string(),
        status: "ok".to_string(),
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
    let producer = match kafka.producer {
        Some(ref producer) => producer,
        None => return HttpResponse::InternalServerError().finish(),
    };

    let topic = &kafka.config.config_topic;
    let key = body.name.as_bytes();
    let message = body.config.to_string();
    let message = message.as_bytes();

    let record = FutureRecord::to(topic).key(key).payload(message);
    let status = producer.send(record, Duration::from_secs(0)).await;

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
