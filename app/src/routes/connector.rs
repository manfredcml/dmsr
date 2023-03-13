use crate::AppState;
use actix_web::{get, post, web, HttpResponse};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize, Serialize, Debug)]
struct GetResponse {
    version: String,
    status: String,
}

#[get("/connectors")]
pub async fn get_connectors(app_data: web::Data<AppState>) -> HttpResponse {
    let config = &app_data.kafka.config;
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
pub async fn post_connectors(body: web::Json<PostBody>) -> HttpResponse {
    println!("body: {:?}", body);

    let config = &body.config;
    println!("config: {:?}", config);

    HttpResponse::Ok().json(body)
}
