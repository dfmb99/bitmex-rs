pub mod api_request;

use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::collections::HashMap;
use std::str;
use crate::utils::enums::{HttpMethod, RequestResponseErr};
use url::Url;
use api_request::ApiRequest;
use awc::{Client, ClientResponse, ClientRequest};
use awc::error::SendRequestError;
use http::StatusCode;
use actix_http::{PayloadStream, Payload};
use actix_http::http::header::HeaderMap;
use actix_http::encoding::Decoder;
use serde_json::Value;
use std::thread::sleep;
use crate::utils::auth::{generate_signature, AuthData};
use log::warn;

const URL_MAIN: &str = "https://www.bitmex.com/api/v1";
const URL_TEST: &str = "https://testnet.bitmex.com/api/v1";
const ERR_SYS_TIME: &str = "Time went backwards";
const ERR_PARSE_URL: &str = "Error parsing url of request";
const ERR_PARSE_JSON: &str = "Error parsing body of request to json";
const SLEEP_ERR_MS: u64 = 2000;
const MAX_RETRIES: u8 = 30;

// type of response of http request with actix web client
type ClientResponseResult = Result<ClientResponse<Decoder<Payload<PayloadStream>>>, SendRequestError>;
// type of successful response of http request with actix web client
type ClientResponseType = ClientResponse<Decoder<Payload<PayloadStream>>>;

pub struct BitmexRest<'a> {
    base_uri: &'a str,
    awc: Client,
    auth_data: AuthData<'a>,
}

impl<'a> BitmexRest<'a> {
    pub async fn new(base_uri: bool, auth_data: AuthData<'a>) -> BitmexRest<'a> {
        BitmexRest {
            base_uri: if base_uri { URL_TEST } else { URL_MAIN },
            awc: Client::default(),
            auth_data,
        }
    }

    async fn make_request(&self, req: ApiRequest<'_>) -> Result<Value, RequestResponseErr> {
        let mut response_map: Result<Value, RequestResponseErr> = Ok(Value::Null);
        for _ in 1..=MAX_RETRIES {
            let mut url = Url::parse(&(self.base_uri.to_owned() + req.endpoint())).expect(ERR_PARSE_URL);
            // generates complete url w/ path params
            for (key, value) in req.query() {
                #[allow(unused_assignments)]
                let mut value_string = String::new();
                // transform Value into string
                if let Some(val_str) = value.as_str() {
                    value_string = val_str.to_string();
                } else {
                    value_string = value.to_string();
                }
                url.query_pairs_mut().append_pair(key, &value_string[..]);
            }
            let body;
            // transforms hashmap of body, if it has entries, into raw string
            if req.body().len() > 0 {
                body = serde_json::to_string(req.body()).expect(ERR_PARSE_JSON);
            } else {
                body = String::new();
            }

            let mut headers = HashMap::new();
            // timestamp after which the request is no longer valid.
            let expires = (get_unix_time_secs() + 5).to_string();
            // signature of request
            let sig;
            match self.auth_data {
                AuthData::Data { key, secret } => {
                    let mut path = String::from(url.path());
                    // If request has query, update path
                    if let Some(query) = url.query() {
                        path = path + "?" + query;
                    }
                    sig = generate_signature(secret, req.method().value(), &path[..], &expires[..], &body[..]);
                    headers.insert("api-expires", &expires[..]);
                    headers.insert("api-key", key);
                    headers.insert("api-signature", &sig[..]);
                }
                _ => {}
            }

            let response_result = {
                match req.method() {
                    HttpMethod::Get => self.get_request(url, headers).await,
                    HttpMethod::Post => self.post_request(url, headers, body).await,
                    HttpMethod::Put => self.put_request(url, headers, body).await,
                    HttpMethod::Delete => self.delete_request(url, headers, body).await,
                }
            };

            match response_result {
                // response has status code ok, parse and return body
                Ok(response) if response.status() == StatusCode::OK => {
                    response_map = Ok(json_to_value(response).await);
                    break;
                }
                // response has status code not ok, handle error
                Ok(response) => {
                    let retry = self.err_handler(&req, response.status(), &response.headers());
                    let err_resp = json_to_value(response).await;
                    response_map = Err(RequestResponseErr::Data(err_resp));
                    if !retry { break; }
                }
                // response not received, SendRequestError occurred
                Err(req_err) => {
                    response_map = Err(RequestResponseErr::SendRequestError(req_err));
                }
            }
        }
        response_map
    }

    fn err_handler(&self, req: &ApiRequest<'_>, status_code: StatusCode, headers: &HeaderMap) -> bool {
        warn!("Rest API error: {} - {} {}", status_code.as_str(), req.method().value(), req.endpoint());
        match status_code {
            StatusCode::SERVICE_UNAVAILABLE => {
                sleep(Duration::from_millis(SLEEP_ERR_MS));
                true
            }
            StatusCode::NOT_FOUND => {
                match req.method() {
                    // order not found, do not retry request
                    &HttpMethod::Delete => false,
                    _ => {
                        sleep(Duration::from_millis(SLEEP_ERR_MS));
                        true
                    }
                }
            }
            StatusCode::TOO_MANY_REQUESTS => {
                let ratelimit_reset: u64 = headers.get("x-ratelimit-reset").unwrap().to_str().unwrap().parse::<u64>().unwrap();
                let to_sleep: u64 = ratelimit_reset - get_unix_time_secs();
                sleep(Duration::from_secs(to_sleep));
                true
            }
            // unknown error, do not retry request
            _ => {
                sleep(Duration::from_millis(SLEEP_ERR_MS));
                false
            }
        }
    }

    async fn get_request(&self, url: Url, headers: HashMap<&str, &str>) -> ClientResponseResult {
        let mut req = self.awc.get(&url[..]);
        req = add_headers(req, headers);
        req.send()
            .await
            .and_then(|response| {
                Ok(response)
            })
    }

    async fn post_request(&self, url: Url, headers: HashMap<&str, &str>, body: String) -> ClientResponseResult {
        let mut req = self.awc.post(&url[..]);
        req = add_headers(req, headers);
        req.send_body(&body)
            .await
            .and_then(|response| {
                Ok(response)
            })
    }

    async fn put_request(&self, url: Url, headers: HashMap<&str, &str>, body: String) -> ClientResponseResult {
        let mut req = self.awc.put(&url[..]);
        req = add_headers(req, headers);
        req.send_body(&body)
            .await
            .and_then(|response| {
                Ok(response)
            })
    }

    async fn delete_request(&self, url: Url, headers: HashMap<&str, &str>, body: String) -> ClientResponseResult {
        let mut req = self.awc.delete(&url[..]);
        req = add_headers(req, headers);
        req.send_body(&body)
            .await
            .and_then(|response| {
                Ok(response)
            })
    }

    /// This returns all instruments and indices, including those that have settled or are unlisted. Use this endpoint if you want to query for individual instruments or use a complex filter. Use /instrument/active to return active instruments, or use a filter like {"state": "Open"}.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_instrument(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/instrument", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get all active instruments and instruments that have expired in <24hrs.
    ///
    /// returns array of objects
    pub async fn get_instrument_active(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/instrument/active", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Helper method. Gets all active instruments and all indices. This is a join of the result of /indices and /active.
    ///
    /// returns array of objects
    pub async fn get_instrument_active_indices(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/instrument/activeAndIndices", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Return all active contract series and interval pairs.
    /// This endpoint is useful for determining which pairs are live. It returns two arrays of strings. The first is intervals, such as ["XBT:perpetual", "XBT:quarterly", "XBT:biquarterly", "ETH:quarterly", ...]. These identifiers are usable in any query's symbol param. The second array is the current resolution of these intervals. Results are mapped at the same index.
    ///
    /// returns array of objects
    pub async fn get_instrument_active_intervals(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/instrument/activeIntervals", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Show constituent parts of an index.
    /// Composite indices are built from multiple external price sources.
    /// Use this endpoint to get the underlying prices of an index. For example, send a symbol of .XBT to get the ticks and weights of the constituent exchanges that build the ".XBT" index.
    /// A tick with reference "BMI" and weight null is the composite index tick.
    ///
    /// returns array of objects
    pub async fn get_instrument_composite_index(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/instrument/compositeIndex", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get all price indices.
    ///
    /// returns array of objects
    pub async fn get_instrument_indices(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/instrument/indices", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get all raw executions for your account.
    /// This returns all raw transactions, which includes order opening and cancelation, and order status changes. It can be quite noisy. More focused information is available at /execution/tradeHistory.
    /// You may also use the filter param to target your query. Specify an array as a filter value, such as {"execType": ["Settlement", "Trade"]} to filter on multiple values.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_execution(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/execution", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get all balance-affecting executions.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_execution_trade_hist(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/execution/tradeHistory", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get your orders.
    /// To get open orders only, send {"open": true} in the filter param.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_order(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/order", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get funding history.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_funding(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/funding", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Create a new order.
    ///
    /// Order Types
    /// All orders require a symbol. All other fields are optional except when otherwise specified.
    /// These are the valid ordTypes:
    /// Limit: The default order type. Specify an orderQty and price.
    /// Market: A traditional Market order. A Market order will execute until filled or your bankruptcy price is reached, at which point it will cancel.
    /// Stop: A Stop Market order. Specify an orderQty and stopPx. When the stopPx is reached, the order will be entered into the book.
    /// On sell orders, the order will trigger if the triggering price is lower than the stopPx. On buys, higher.
    /// Note: Stop orders do not consume margin until triggered. Be sure that the required margin is available in your account so that it may trigger fully.
    /// Close Stops don't require an orderQty. See Execution Instructions below.
    /// StopLimit: Like a Stop Market, but enters a Limit order instead of a Market order. Specify an orderQty, stopPx, and price.
    /// MarketIfTouched: Similar to a Stop, but triggers are done in the opposite direction. Useful for Take Profit orders.
    /// LimitIfTouched: As above; use for Take Profit Limit orders.
    /// Pegged: Pegged orders allow users to submit a limit price relative to the current market price. Specify a pegPriceType, and pegOffsetValue.
    /// Pegged orders must have an execInst of Fixed. This means the limit price is set at the time the order is accepted and does not change as the reference price changes.
    /// PrimaryPeg: Price is set relative to near touch price.
    /// MarketPeg: Price is set relative to far touch price.
    /// A pegPriceType submitted with no ordType is treated as a Pegged order.
    ///
    /// Execution Instructions
    /// The following execInsts are supported. If using multiple, separate with a comma (e.g. LastPrice,Close).
    /// ParticipateDoNotInitiate: Also known as a Post-Only order. If this order would have executed on placement, it will cancel instead.
    /// MarkPrice, LastPrice, IndexPrice: Used by stop and if-touched orders to determine the triggering price. Use only one. By default, MarkPrice is used. Also used for Pegged orders to define the value of LastPeg.
    /// ReduceOnly: A ReduceOnly order can only reduce your position, not increase it. If you have a ReduceOnly limit order that rests in the order book while the position is reduced by other orders, then its order quantity will be amended down or canceled. If there are multiple ReduceOnly orders the least aggressive will be amended first.
    /// Close: Close implies ReduceOnly. A Close order will cancel other active limit orders with the same side and symbol if the open quantity exceeds the current position. This is useful for stops: by canceling these orders, a Close Stop is ensured to have the margin required to execute, and can only execute up to the full size of your position. If orderQty is not specified, a Close order has an orderQty equal to your current position's size.
    /// Note that a Close order without an orderQty requires a side, so that BitMEX knows if it should trigger above or below the stopPx.
    /// LastWithinMark: Used by stop orders with LastPrice to allow stop triggers only when:
    /// For Sell Stop Market / Stop Limit Order
    /// Last Price <= Stop Price
    /// Last Price >= Mark Price × (1 - 5%)
    /// For Buy Stop Market / Stop Limit Order:
    /// Last Price >= Stop Price
    /// Last Price <= Mark Price × (1 + 5%)
    /// Fixed: Pegged orders must have an execInst of Fixed. This means the limit price is set at the time the order is accepted and does not change as the reference price changes.
    ///
    /// Pegged Orders
    /// Pegged orders allow users to submit a limit price relative to the current market price. The limit price is set at the time the order is accepted and does not change as the reference price changes.
    /// Pegged orders have an ordType of Pegged, and an execInst of Fixed.
    /// A pegPriceType and pegOffsetValue must also be submitted:
    /// PrimaryPeg - price is set relative to the near touch price
    /// MarketPeg - price is set relative to the far touch price
    ///
    /// Trailing Stop Pegged Orders
    /// Use pegPriceType of TrailingStopPeg to create Trailing Stops.
    /// The price is set at submission and updates once per second if the underlying price (last/mark/index) has moved by more than 0.1%. stopPx then moves as the market moves away from the peg, and freezes as the market moves toward it.
    /// Use pegOffsetValue to set the stopPx of your order. The peg is set to the triggering price specified in the execInst (default MarkPrice). Use a negative offset for stop-sell and buy-if-touched orders.
    /// Requires ordType: Stop, StopLimit, MarketIfTouched, LimitIfTouched.
    ///
    /// Trailing Stops
    /// You may use pegPriceType of 'TrailingStopPeg' to create Trailing Stops. The pegged stopPx will move as the market moves away from the peg, and freeze as the market moves toward it.
    /// To use, combine with pegOffsetValue to set the stopPx of your order. The peg is set to the triggering price specified in the execInst (default 'MarkPrice'). Use a negative offset for stop-sell and buy-if-touched orders.
    /// Requires ordType: 'Stop', 'StopLimit', 'MarketIfTouched', 'LimitIfTouched'.
    ///
    /// Rate Limits
    /// See the Bulk Order Documentation if you need to place multiple orders at the same time. Bulk orders require fewer risk checks in the trading engine and thus are ratelimited at 1/10 the normal rate.
    /// You can also improve your reactivity to market movements while staying under your ratelimit by using the Amend and Amend Bulk endpoints. This allows you to stay in the market and avoids the cancel/replace cycle.
    ///
    /// Tracking Your Orders
    /// If you want to keep track of order IDs yourself, set a unique clOrdID per order. This clOrdID will come back as a property on the order and any related executions (including on the WebSocket), and can be used to get or cancel the order. Max length is 36 characters.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. e.g. 'XBTUSD'.
    /// * 'side' (string) - Order side. Valid options: Buy, Sell. Defaults to 'Buy' unless orderQty is negative.
    /// * 'orderQty' (number) - Order quantity in units of the instrument (i.e. contracts).
    /// * 'price' (number) - Optional limit price for 'Limit', 'StopLimit', and 'LimitIfTouched' orders.
    /// * 'displayQty' (number) - Optional quantity to display in the book. Use 0 for a fully hidden order.
    /// * 'stopPx' (number) - Optional trigger price for 'Stop', 'StopLimit', 'MarketIfTouched', and 'LimitIfTouched' orders. Use a price below the current price for stop-sell orders and buy-if-touched orders. Use execInst of 'MarkPrice' or 'LastPrice' to define the current price used for triggering.
    /// * 'clOrdID' (string) - Optional Client Order ID. This clOrdID will come back on the order and any related executions.
    /// * 'pegOffsetValue' (number) - Optional trailing offset from the current price for 'Stop', 'StopLimit', 'MarketIfTouched', and 'LimitIfTouched' orders; use a negative offset for stop-sell orders and buy-if-touched orders. Optional offset from the peg price for 'Pegged' orders.
    /// * 'pegPriceType' (number) - Optional peg price type. Valid options: MarketPeg, PrimaryPeg, TrailingStopPeg.
    /// * 'ordType' (string) - Order type. Valid options: Market, Limit, Stop, StopLimit, MarketIfTouched, LimitIfTouched, Pegged. Defaults to 'Limit' when price is specified. Defaults to 'Stop' when stopPx is specified. Defaults to 'StopLimit' when price and stopPx are specified.
    /// * 'timeInForce' (string) - Time in force. Valid options: Day, GoodTillCancel, ImmediateOrCancel, FillOrKill. Defaults to 'GoodTillCancel' for 'Limit', 'StopLimit', and 'LimitIfTouched' orders.
    /// * 'execInst' (string) - Optional execution instructions. Valid options: ParticipateDoNotInitiate, AllOrNone, MarkPrice, IndexPrice, LastPrice, Close, ReduceOnly, Fixed, LastWithinMark. 'AllOrNone' instruction requires displayQty to be 0. 'MarkPrice', 'IndexPrice' or 'LastPrice' instruction valid for 'Stop', 'StopLimit', 'MarketIfTouched', and 'LimitIfTouched' orders. 'LastWithinMark' instruction valid for 'Stop' and 'StopLimit' with instruction 'LastPrice'.
    /// * 'text' (string) - Optional order annotation. e.g. 'Take profit'.
    ///
    /// returns object
    pub async fn place_order(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/order", HttpMethod::Post, HashMap::new(), params)).await
    }

    /// Amend the quantity or price of an open order.
    /// Send an orderID or origClOrdID to identify the order you wish to amend.
    /// Both order quantity and price can be amended. Only one qty field can be used to amend.
    /// Use the leavesQty field to specify how much of the order you wish to remain open. This can be useful if you want to adjust your position's delta by a certain amount, regardless of how much of the order has already filled.
    /// > A leavesQty can be used to make a "Filled" order live again, if it is received within 60 seconds of the fill.
    /// Like order placement, amending can be done in bulk. Simply send a request to PUT /api/v1/order/bulk with a JSON body of the shape: {"orders": [{...}, {...}]}, each object containing the fields used in this endpoint.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'orderID' (string) - Order ID
    /// * 'origClOrdID' (string) - Client Order ID. See POST /order
    /// * 'clOrdID' (string) - Optional new Client Order ID, requires origClOrdID.
    /// * 'orderQty' (number) - Optional order quantity in units of the instrument (i.e. contracts).
    /// * 'leavesQty' (number) - Optional leaves quantity in units of the instrument (i.e. contracts). Useful for amending partially filled orders.
    /// * 'price' (number) - Optional limit price for 'Limit', 'StopLimit', and 'LimitIfTouched' orders.
    /// * 'stopPx' (number) - Optional trigger price for 'Stop', 'StopLimit', 'MarketIfTouched', and 'LimitIfTouched' orders. Use a price below the current price for stop-sell orders and buy-if-touched orders.
    /// * 'pegOffsetValue' (number) - Optional trailing offset from the current price for 'Stop', 'StopLimit', 'MarketIfTouched', and 'LimitIfTouched' orders; use a negative offset for stop-sell orders and buy-if-touched orders. Optional offset from the peg price for 'Pegged' orders.
    /// * 'text' (string) - Optional amend annotation. e.g. 'Adjust skew'.
    ///
    /// returns object
    pub async fn amend_order(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/order", HttpMethod::Put, HashMap::new(), params)).await
    }

    /// Cancel order(s). Send multiple order IDs to cancel in bulk.
    /// Either an orderID or a clOrdID must be provided.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'orderID' (string) - Order ID(s).
    /// * 'clOrdID' (string) - Client Order ID(s). See POST /order.
    /// * 'text' (string) - Optional cancellation annotation. e.g. 'Spread Exceeded'.
    ///
    /// returns array of objects
    pub async fn cancel_orders(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/order", HttpMethod::Delete, HashMap::new(), params)).await
    }

    /// Cancels all of your orders.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Optional symbol. If provided, only cancels orders for that symbol.
    /// * 'filter' (string) - Optional filter for cancellation. Use to only cancel some orders, e.g. {"side": "Buy"}.
    /// * 'text' (string) - Optional cancellation annotation. e.g. 'Spread Exceeded'
    ///
    /// returns array of objects
    pub async fn cancel_all_orders(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/order/all", HttpMethod::Delete, HashMap::new(), params)).await
    }

    /// Amend multiple orders for the same symbol.
    /// Similar to POST /amend, but with multiple orders. application/json only. Ratelimited at 10% of the per minute rate limiter.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'orders' (string) - An array of orders.
    ///
    /// returns array of objects
    pub async fn amend_bulk_orders(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/order/bulk", HttpMethod::Put, HashMap::new(), params)).await
    }

    /// Create multiple new orders for the same symbol.
    /// This endpoint is used for placing bulk orders. Valid order types are Limit, Stop, StopLimit, MarketIfTouched, LimitIfTouched, and Pegged.
    /// Each individual order object in the array should have the same properties as an individual POST /order call.
    /// This endpoint is much faster for getting many orders into the book at once. Because it reduces load on BitMEX systems, this endpoint is rate limited at ceil(0.1 * orders) on the per minute limiter. Submitting 10 orders via a bulk order call will only count as 1 request, 15 as 2, 32 as 4, and so on.
    /// Bulk requests are not scored against the per second limiter and always deduct 1 token for every request.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'orders' (string) - An array of orders.
    ///
    /// returns array of objects
    pub async fn place_bulk_orders(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/order/bulk", HttpMethod::Post, HashMap::new(), params)).await
    }

    /// Automatically cancel all your orders after a specified timeout.
    /// Useful as a dead-man's switch to ensure your orders are canceled in case of an outage. If called repeatedly, the existing offset will be canceled and a new one will be inserted in its place.
    /// Example usage: call this route at 15s intervals with an offset of 60000 (60s). If this route is not called within 60 seconds, all your orders will be automatically canceled.
    /// This is also available via WebSocket.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'timeout' (string) - Timeout in ms. Set to 0 to cancel this timer.
    ///
    /// returns object
    pub async fn cancel_all_orders_after(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/order/cancelAllAfter", HttpMethod::Post, HashMap::new(), params)).await
    }

    /// Get current orderbook in vertical format.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a series (e.g. XBT) to get data for the nearest contract in that series.
    /// * 'depth' (number) - Orderbook depth per side. Send 0 for full depth.
    ///
    /// returns object
    pub async fn get_orderbook_l2(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/orderBook/L2", HttpMethod::Get, HashMap::new(), params)).await
    }

    /// Get your positions.
    /// This endpoint is used for retrieving position information. The fields largely follow the FIX spec definitions. Some selected fields are explained in more detail below.
    /// The fields account, symbol, currency are unique to each position and form its key.
    ///
    /// account: Your unique account ID.
    /// symbol: The contract for this position.
    /// currency: The margin currency for this position.
    /// underlying: Meta data of the symbol.
    /// quoteCurrency: Meta data of the symbol, All prices are in the quoteCurrency
    /// commission: The maximum of the maker, taker, and settlement fee.
    /// initMarginReq: The initial margin requirement. This will be at least the symbol's default initial maintenance margin, but can be higher if you choose lower leverage.
    /// maintMarginReq: The maintenance margin requirement. This will be at least the symbol's default maintenance maintenance margin, but can be higher if you choose a higher risk limit.
    /// riskLimit: This is a function of your maintMarginReq.
    /// leverage: 1 / initMarginReq.
    /// crossMargin: True/false depending on whether you set cross margin on this position.
    /// deleveragePercentile: Indicates where your position is in the ADL queue.
    /// rebalancedPnl: The value of realised PNL that has transferred to your wallet for this position.
    /// prevRealisedPnl: The value of realised PNL that has transferred to your wallet for this position since the position was closed.
    /// currentQty: The current position amount in contracts.
    /// currentCost: The current cost of the position in the settlement currency of the symbol (currency).
    /// currentComm: The current commission of the position in the settlement currency of the symbol (currency).
    /// realisedCost: The realised cost of this position calculated with regard to average cost accounting.
    /// unrealisedCost: currentCost - realisedCost.
    /// grossOpenCost: The absolute value of your open orders for this symbol.
    /// grossOpenPremium: The amount your bidding above the mark price in the settlement currency of the symbol (currency).
    /// markPrice: The mark price of the symbol in quoteCurrency.
    /// markValue: The currentQty at the mark price in the settlement currency of the symbol (currency).
    /// homeNotional: Value of position in units of underlying.
    /// foreignNotional: Value of position in units of quoteCurrency.
    /// realisedPnl: The negative of realisedCost.
    /// unrealisedGrossPnl: markValue - unrealisedCost.
    /// unrealisedPnl: unrealisedGrossPnl.
    /// liquidationPrice: Once markPrice reaches this price, this position will be liquidated.
    /// bankruptPrice: Once markPrice reaches this price, this position will have no equity.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'filter' (string) - Table filter. For example, send {"symbol": "XBTUSD"}.
    /// * 'columns' (string) - Which columns to fetch. For example, send ["columnName"].
    /// * 'count' (string) - Number of rows to fetch.
    ///
    /// returns array of objects
    pub async fn get_position(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/position", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Enable isolated margin or cross margin per-position.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Position symbol to isolate.
    /// * 'enabled' (boolean) - True for isolated margin, false for cross margin.
    ///
    /// returns object
    pub async fn choose_position_margin(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/position/isolate", HttpMethod::Post, params, HashMap::new())).await
    }

    /// Choose leverage for a position.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Symbol of position to adjust.
    /// * 'leverage' (number) - Leverage value. Send a number between 0.01 and 100 to enable isolated margin with a fixed leverage. Send 0 to enable cross margin.
    ///
    /// returns object
    pub async fn choose_position_leverage(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/position/leverage", HttpMethod::Post, params, HashMap::new())).await
    }

    /// Transfer equity in or out of a position.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Symbol of position to isolate.
    /// * 'amount' (number) - Amount to transfer, in Satoshis. May be negative.
    ///
    /// returns object
    pub async fn transfer_position_margin(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/position/transferMargin", HttpMethod::Post, params, HashMap::new())).await
    }

    /// Update your risk limit.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Symbol of position to update risk limit on.
    /// * 'riskLimit' (number) - New Risk Limit, in Satoshis.
    ///
    /// returns object
    pub async fn choose_position_risklimit(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/position/leverage", HttpMethod::Post, params, HashMap::new())).await
    }

    /// Get Quotes.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_quote(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/quote", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get previous quotes in time buckets.
    /// Timestamps returned by our bucketed endpoints are the end of the period, indicating when the bucket was written to disk. Some other common systems use the timestamp as the beginning of the period. Please be aware of this when using this endpoint.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'binSize' (string) - Time interval to bucket by. Available options: [1m,5m,1h,1d].
    /// * 'partial' (boolean) - If true, will send in-progress (incomplete) bins for the current time period.
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_quote_bucketed(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/quote/bucketed", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get settlement history.
    /// Timestamps returned by our bucketed endpoints are the end of the period, indicating when the bucket was written to disk. Some other common systems use the timestamp as the beginning of the period. Please be aware of this when using this endpoint.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_settlement_hist(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/settlement", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get exchange-wide and per-series turnover and volume statistics.
    ///
    /// returns array of objects
    pub async fn get_stats(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/stats", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get Trades.
    /// Please note that indices (symbols starting with .) post trades at intervals to the trade feed. These have a size of 0 and are used only to indicate a changing price.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_trades(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/trade", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get previous trades in time buckets.
    /// Timestamps returned by our bucketed endpoints are the end of the period, indicating when the bucket was written to disk. Some other common systems use the timestamp as the beginning of the period. Please be aware of this when using this endpoint.
    /// Also note the open price is equal to the close price of the previous timeframe bucket.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'binSize' (string) - Time interval to bucket by. Available options: [1m,5m,1h,1d].
    /// * 'partial' (boolean) - If true, will send in-progress (incomplete) bins for the current time period.
    /// * 'symbol' (string) - Instrument symbol. Send a bare series (e.g. XBT) to get data for the nearest expiring contract in that series.
    /// * 'filter' (string) - Generic table filter. Send JSON key/value pairs, such as {"key": "value"}. You can key on individual fields, and do more advanced querying on timestamps
    /// * 'columns' (string) - Array of column names to fetch. If omitted, will return all columns.
    /// * 'count' (number) - Number of results to fetch. Must be a positive integer.
    /// * 'start' (number) - Starting point for results.
    /// * 'reverse' (boolean) - If true, will sort results newest first.
    /// * 'startTime' (string) - Starting date filter for results.
    /// * 'endTime' (string) - Ending date filter for results.
    ///
    /// returns array of objects
    pub async fn get_trades_bucketed(&self, params: HashMap<&str, Value>) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/trade/bucketed", HttpMethod::Get, params, HashMap::new())).await
    }

    /// Get your user model.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'currency' (string)
    ///
    /// returns object
    pub async fn get_user(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/user", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get your account's margin status. Send a currency of "all" to receive an array of all supported currencies.
    ///
    /// Hashmap parameter keys:
    ///
    /// * 'currency' (string)
    ///
    /// returns object
    pub async fn get_user_margin(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/user/margin", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get your current wallet information.
    ///
    /// returns object
    pub async fn get_user_wallet(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/user/wallet", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get 7 days worth of Quote Fill Ratio statistics.
    ///
    /// returns object
    pub async fn get_user_quote_fill_ratio(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/user/quoteFillRatio", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get Quote Value Ratio statistics over the last 3 days
    ///
    /// returns object
    pub async fn get_user_quote_value_ratio(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/user/quoteValueRatio", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get a history of all of your wallet transactions (deposits, withdrawals, PNL).
    ///
    /// returns array of objects
    pub async fn get_user_wallet_history(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/user/walletHistory", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

    /// Get a summary of all of your wallet transactions (deposits, withdrawals, PNL).
    ///
    /// returns array of objects
    pub async fn get_user_wallet_summary(&self) -> Result<Value, RequestResponseErr> {
        self.make_request(ApiRequest::new("/user/walletSummary", HttpMethod::Get, HashMap::new(), HashMap::new())).await
    }

}

async fn json_to_value(mut client_resp: ClientResponseType) -> Value {
    let body_string = String::from_utf8(client_resp.body().await.unwrap_or_default().to_vec()).unwrap_or_default();
    // never panics
    serde_json::from_str(&body_string[..]).unwrap_or_default()
}

fn add_headers(mut req: ClientRequest, headers: HashMap<&str, &str>) -> ClientRequest {
    req = req.header("content-type", "application/json");
    for (key, value) in headers {
        req = req.header(key, value);
    }
    req
}

fn get_unix_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect(ERR_SYS_TIME).as_secs()
}

#[cfg(test)]
mod tests {
    use crate::rest::{BitmexRest};
    use crate::utils::enums::{HttpMethod, RequestResponseErr};
    use crate::rest::api_request::ApiRequest;
    use std::collections::HashMap;
    use actix_rt::System;
    use serde_json::{Value, Map};
    use serde_json::value::Value::Number;
    use crate::utils::auth::{AuthData};

    #[test]
    fn test_instrument() {
        System::new("test").block_on(async {
            let mex: BitmexRest = BitmexRest::new(true, AuthData::None).await;

            let mut value: Map<String, Value> = Map::new();
            value.insert(String::from("symbol"), Value::from("XBTUSD"));

            let mut params: HashMap<&str, Value> = HashMap::new();
            params.insert("filter", Value::Object(value));

            if let Err(err) = mex.get_instrument(params).await {
                match err {
                    RequestResponseErr::Data(data) => { panic!("{:?}", data) }
                    RequestResponseErr::SendRequestError(req_err) => { panic!("{:?}", req_err) }
                }
            }
            if let Err(err) = mex.get_instrument_active().await {
                match err {
                    RequestResponseErr::Data(data) => { panic!("{:?}", data) }
                    RequestResponseErr::SendRequestError(req_err) => { panic!("{:?}", req_err) }
                }
            }
        });
    }

    #[test]
    fn test_execution_request() {
        System::new("test").block_on(async {
            let c: BitmexRest = BitmexRest::new(true, AuthData::Data { key: "TmiigTrWSU0Z2D2z87kT_z0S", secret: "faF_FH51vwmapKKDUxGAw2f4pxmTdtWKwDcwk-NspZfQjsqh" }).await;

            let params: HashMap<&str, Value> = HashMap::new();
            if let Err(err) = c.get_execution(params).await {
                match err {
                    RequestResponseErr::Data(data) => { panic!("{:?}", data) }
                    RequestResponseErr::SendRequestError(req_err) => { panic!("{:?}", req_err) }
                }
            }

            let params: HashMap<&str, Value> = HashMap::new();
            if let Err(err) = c.get_execution_trade_hist(params).await {
                match err {
                    RequestResponseErr::Data(data) => { panic!("{:?}", data) }
                    RequestResponseErr::SendRequestError(req_err) => { panic!("{:?}", req_err) }
                }
            }
        });
    }

    #[test]
    fn test_order_requests() {
        System::new("test").block_on(async {
            let c: BitmexRest = BitmexRest::new(true, AuthData::Data { key: "r1PILWvnD85O2fSqaAWtyzD7", secret: "9eCfwZRAemUwcoZXlW0AlShDMhOYaSr4cc989d2S5fJpwLa_" }).await;

            let mut params: HashMap<&str, Value> = HashMap::new();
            params.insert("symbol", Value::from("XBTUSD"));
            params.insert("ordType", Value::from("Limit"));
            params.insert("price", Value::from(10));
            params.insert("orderQty", Value::from(1));

            let mut params1: HashMap<&str, Value> = HashMap::new();
            params1.insert("symbol", Value::from("XBTUSD"));
            params1.insert("ordType", Value::from("Limit"));
            params1.insert("price", Value::from(10));
            params1.insert("orderQty", Value::from(1));

            let mut v = Vec::new();
            v.insert(0,serde_json::to_string(&params).unwrap());
            v.insert(1,serde_json::to_string(&params1).unwrap());

            let mut p: HashMap<&str, Value> = HashMap::new();
            p.insert("orders", Value::from(v));

            if let Err(err) = c.place_bulk_orders(p).await{
                match err {
                    RequestResponseErr::Data(data) => { panic!("{:?}", data) }
                    RequestResponseErr::SendRequestError(req_err) => { panic!("{:?}", req_err) }
                }
            }
        });
    }

    #[test]
    fn test_user_requests() {
        System::new("test").block_on(async {
            let c: BitmexRest = BitmexRest::new(true, AuthData::Data { key: "TmiigTrWSU0Z2D2z87kT_z0S", secret: "faF_FH51vwmapKKDUxGAw2f4pxmTdtWKwDcwk-NspZfQjsqh" }).await;

            if let Err(err) = c.get_user_margin().await {
                match err {
                    RequestResponseErr::Data(data) => { panic!("{:?}", data) }
                    RequestResponseErr::SendRequestError(req_err) => { panic!("{:?}", req_err) }
                }
            }
        });
    }
}

