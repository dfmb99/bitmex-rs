use serde::{Deserialize, Serialize, Deserializer};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub leverage: u64,
    #[serde(rename = "maintMargin")]
    pub maint_margin: u64,
    #[serde(rename = "crossMargin")]
    pub cross_margin: bool,
    #[serde(rename = "realisedPnl")]
    pub realised_pnl: i64,
    #[serde(rename = "unrealisedPnl")]
    pub unrealised_pnl: i64,
    #[serde(rename = "currentQty")]
    pub current_qty: i64,
    #[serde(deserialize_with = "deserialize_null_default")]
    #[serde(rename = "avgEntryPrice")]
    pub avg_entry_price: f64,
    #[serde(deserialize_with = "deserialize_null_default")]
    #[serde(rename = "liquidationPrice")]
    pub liquidation_price: f64,
    pub timestamp: String,
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        T: Default + Deserialize<'de>,
        D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}