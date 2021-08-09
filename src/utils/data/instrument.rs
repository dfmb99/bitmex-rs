use serde::{Deserialize, Serialize, Deserializer};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Instrument {
    pub symbol: String,
    #[serde(rename = "isQuanto")]
    pub is_quanto: bool,
    #[serde(rename = "isInverse")]
    pub is_inverse: bool,
    pub multiplier: i64,
    #[serde(deserialize_with = "deserialize_null_default")]
    pub expiry: String,
    #[serde(rename = "tickSize")]
    pub tick_size: f64,
    #[serde(rename = "maxPrice")]
    pub max_price: f64,
    #[serde(rename = "maintMargin")]
    pub maint_margin: f64,
    #[serde(rename = "fundingRate")]
    pub funding_rate: f64,
    #[serde(rename = "indicativeFundingRate")]
    pub indicative_funding_rate: f64,
    pub volume24h: u64,
    pub vwap: f64,
    #[serde(rename = "markPrice")]
    pub mark_price: f64,
    #[serde(rename = "lastPrice")]
    pub last_price: f64,
    #[serde(rename = "bidPrice")]
    pub bid_price: f64,
    #[serde(rename = "midPrice")]
    pub mid_price: f64,
    #[serde(rename = "askPrice")]
    pub ask_price: f64,
    #[serde(rename = "impactBidPrice")]
    pub impact_bid_price: f64,
    #[serde(rename = "impactMidPrice")]
    pub impact_mid_price: f64,
    #[serde(rename = "impactAskPrice")]
    pub impact_ask_price: f64,
    #[serde(rename = "openInterest")]
    pub open_interest: u64,
    #[serde(rename = "openValue")]
    pub open_value: u64,
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