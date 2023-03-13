use actix_web::{get, HttpResponse};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
struct IndexResponse {
    version: String,
    status: String,
}

#[get("/")]
pub async fn index() -> HttpResponse {
    let resp = IndexResponse {
        version: "0.0.1".to_string(),
        status: "ok".to_string(),
    };
    HttpResponse::Ok().json(resp)
}
