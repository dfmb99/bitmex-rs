use serde::{Deserialize, Serialize, Deserializer};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TradeBucketed {
    pub timestamp: String,
    pub symbol: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub trades: u64,
    pub volume: u64,
    #[serde(deserialize_with = "deserialize_null_default")]
    pub vwap: f64,
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        T: Default + Deserialize<'de>,
        D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}
