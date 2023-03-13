use actix_web::{get, post, web, HttpResponse};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
struct GetResponse {
    version: String,
    status: String,
}

#[get("/connectors")]
pub async fn get_connectors() -> HttpResponse {
    let resp = GetResponse {
        version: "0.0.1".to_string(),
        status: "ok".to_string(),
    };
    HttpResponse::Ok().json(resp)
}

#[derive(Deserialize, Serialize, Debug)]
struct PostBody {
    name: String,
    config: serde_json::Value,
}

#[post("/connectors")]
pub async fn post_connectors(body: web::Json<PostBody>) -> HttpResponse {
    println!("body: {:?}", body);
    HttpResponse::Ok().json(body)
}
