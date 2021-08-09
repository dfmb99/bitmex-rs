use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Execution {
    #[serde(rename = "execID")]
    pub exec_id: String,
    #[serde(rename = "orderID")]
    pub order_id: String,
    #[serde(rename = "clOrdID")]
    pub clord_id: String,
    pub symbol: String,
    pub side: String,
    #[serde(rename = "orderQty")]
    pub order_qty: u64,
    pub price: f64,
    #[serde(rename = "ordType")]
    pub ord_type: String,
    #[serde(rename = "ordStatus")]
    pub ord_status: String,
    #[serde(rename = "leavesQty")]
    pub leaves_qty: u64,
    pub timestamp: String,
}