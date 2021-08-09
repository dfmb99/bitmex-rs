use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderBookL2 {
    pub symbol: String,
    pub side: String,
    pub id: u64,
    pub size: u64,
    pub price: f64,
}