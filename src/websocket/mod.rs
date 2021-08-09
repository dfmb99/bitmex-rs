use tungstenite::{Message, connect, WebSocket};
use tungstenite::client::AutoStream;
use url::Url;
use std::collections::HashMap;
use serde_json::{Value, Map, json};
use crate::utils::auth::{generate_signature, AuthData};
use crate::rest::BitmexRest;
use std::sync::{RwLock, Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use log::{error, info, debug, warn};
use crate::utils::enums::Subscriptions;
use crate::utils::thread_pool::ThreadPool;
use std::thread;
use rayon::prelude::*;
use chrono::{DateTime, Utc};
use crate::utils::data::order::Order;
use crate::utils::data::position::Position;
use crate::utils::data::instrument::Instrument;
use crate::utils::data::trade_bucketed::TradeBucketed;
use crate::utils::data::order_book_l2::OrderBookL2;
use crate::utils::data::execution::Execution;
use crate::utils::data::user_margin::Margin;
use actix_rt::System;

const URL_MAIN: &str = "wss://www.bitmex.com/realtime";
const URL_TEST: &str = "wss://testnet.bitmex.com/realtime";
const LEN_ORDERS: usize = 40;
const LEN_TRADE_BUCKETED: usize = 720;
const LEN_DEFAULT: usize = 100;
const CONNECT_ERR_SLEEP_MS: u64 = 1000;
const DELAY_RC: i64 = 15;
const ERR_SYS_TIME: &str = "Time went backwards";

type Socket = Arc<Mutex<WebSocket<AutoStream>>>;
type Keys = Arc<RwLock<HashMap<String, Vec<String>>>>;
type Data = Arc<RwLock<HashMap<String, Vec<Map<String, Value>>>>>;
// vec that stores all the ordered ids of the table "orderBookL2"
type L2ids = Arc<RwLock<Vec<u64>>>;

pub struct BitmexWs {
    testnet: bool,
    symbol: &'static str,
    thread_pool_size: usize,
    subscriptions: Vec<Subscriptions>,
    socket: Socket,
    auth_data: AuthData<'static>,
    keys: Keys,
    data: Data,
    l2_ids: L2ids,
}

impl BitmexWs {
    pub async fn new(testnet: bool, symbol: &'static str, thread_pool_size: usize, subscriptions: Vec<Subscriptions>, auth_data: AuthData<'static>) -> BitmexWs {
        let mut ws = BitmexWs {
            socket: Arc::new(Mutex::new(connect_ws(if testnet { URL_TEST } else { URL_MAIN }, symbol, &auth_data, &subscriptions))),
            keys: Arc::new(RwLock::new(HashMap::with_capacity(5))),
            data: Arc::new(RwLock::new(HashMap::with_capacity(5))),
            l2_ids: Arc::new(RwLock::new(Vec::with_capacity(5))),
            thread_pool_size,
            symbol,
            testnet,
            subscriptions,
            auth_data,
        };
        ws.run().await;
        ws.wait_for_data();
        ws
    }

    fn wait_for_data(&self) {
        while !self.is_data_available() {
            thread::yield_now();
        }
    }

    fn is_data_available(&self) -> bool{
        self.data.read().unwrap().len() == self.subscriptions.len()
    }

    /// Starts websocket connection
    async fn run(&mut self) {

        let thread_pool = ThreadPool::new(self.thread_pool_size);
        let socket_clone = Arc::clone(&self.socket);
        let data_clone = Arc::clone(&self.data);
        let keys_clone = Arc::clone(&self.keys);
        let l2_ids_clone = Arc::clone(&self.l2_ids);
        let symbol = self.symbol;
        let auth_data = self.auth_data;
        let testnet = self.testnet;
        let subscriptions = self.subscriptions.clone();

        // data updates are managed by a thread pool asynchronously
        thread::spawn(move || {
            loop {
                let mut socket = socket_clone.lock().unwrap();
                match socket.read_message() {
                    Ok(msg) => {
                        debug!("Received ws message: {:?}", msg);
                        let map_msg: Map<String, Value> = serde_json::from_str(&msg.to_string()[..]).unwrap_or_default();
                        if let Some(data) = map_msg.get("data") {
                            let keys_memory = Arc::clone(&keys_clone);
                            let data_memory = Arc::clone(&data_clone);
                            let l2_ids_memory = Arc::clone(&l2_ids_clone);
                            let table = map_msg.get("table").unwrap().as_str().unwrap().to_string();
                            let action = map_msg.get("action").unwrap().as_str().unwrap().to_string();
                            let data_rec = data.to_owned();
                            thread_pool.execute(move || {
                                if action == "partial" {
                                    let keys_rec = map_msg.get("keys").unwrap().to_owned();
                                    System::new("env").block_on(async move {
                                        handle_partial(symbol, testnet, auth_data, &table[..], keys_rec, data_rec, data_memory, keys_memory, l2_ids_memory).await;
                                    });
                                } else if data_memory.read().unwrap().contains_key(&table){
                                    handle_data_msg(table, action, data_rec, data_memory, keys_memory, l2_ids_memory);
                                }
                            });
                        } else if let (Some(_), Some(subscription)) = (map_msg.get("success"), map_msg.get("subscribe")) {
                            info!("Subscribed to: {}", subscription.as_str().unwrap());
                        } else if let Some(info) = map_msg.get("info") {
                            info!("{}", info.as_str().unwrap());
                        } else if let Some(error) = map_msg.get("error") {
                            let status = map_msg.get("status").unwrap().as_i64().unwrap();
                            error!("Code: {}, Error: {}", status, error);
                        } else {
                            info!("Unknown message: {:?}", msg);
                        }
                    }
                    Err(err) => {
                        error!("Error reading ws message: {}", err);
                        *socket = connect_ws(if testnet { URL_TEST } else { URL_MAIN }, symbol, &auth_data, &subscriptions);
                    }
                };
            }
        });
    }

    /// Returns size of orders at a given order book level
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `orderBookL2` stream
    pub fn get_order_book_size(&self, price: f64) -> u64 {
        let data = self.data.read().unwrap();
        let book_data = data.get("orderBookL2");
        assert!(book_data.is_some());
        let book_data = book_data.unwrap().clone();
        let mut book_data: Vec<OrderBookL2> = serde_json::from_value(json!(book_data)).unwrap();
        book_data.reverse();
        let location = book_data.binary_search_by(|v| {
            v.price.partial_cmp(&price).unwrap()
        });
        return match location {
            Ok(pos) => book_data.get(pos).unwrap().size,
            _ => 0,
        };
    }

    /// Returns open orders
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `order` stream
    pub fn get_open_orders(&self) -> Vec<Order> {
        let data = self.data.read().unwrap();
        let order_data = data.get("order");
        assert!(order_data.is_some());
        let order_data = order_data.unwrap().clone();
        let order_data: Vec<Order> = serde_json::from_value(json!(order_data)).unwrap();
        order_data.into_iter().filter(|item| item.ord_status == "New" || item.ord_status == "PartiallyFilled").collect()
    }

    /// Returns filled orders
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `order` stream
    pub fn get_filled_orders(&self) -> Vec<Order> {
        let data = self.data.read().unwrap();
        let order_data = data.get("order");
        assert!(order_data.is_some());
        let order_data = order_data.unwrap().clone();
        let order_data: Vec<Order> = serde_json::from_value(json!(order_data)).unwrap();
        order_data.into_iter().filter(|item| item.ord_status == "Filled").collect()
    }

    /// Returns canceled orders
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `order` stream
    pub fn get_canceled_orders(&self) -> Vec<Order> {
        let data = self.data.read().unwrap();
        let order_data = data.get("order");
        assert!(order_data.is_some());
        let order_data = order_data.unwrap().clone();
        let order_data: Vec<Order> = serde_json::from_value(json!(order_data)).unwrap();
        order_data.into_iter().filter(|item| item.ord_status == "Canceled").collect()
    }

    /// Returns position data
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `position` stream
    pub fn get_position(&self) -> Vec<Position> {
        let data = self.data.read().unwrap();
        let position_data = data.get("position");
        assert!(position_data.is_some());
        let position_data = position_data.unwrap().clone();
        serde_json::from_value(json!(position_data)).unwrap()
    }

    /// Returns instrument data
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `instrument` stream
    pub fn get_instrument(&self) -> Vec<Instrument> {
        let data = self.data.read().unwrap();
        let instrument_data = data.get("instrument");
        assert!(instrument_data.is_some());
        let instrument_data = instrument_data.unwrap().clone();
        serde_json::from_value(json!(instrument_data)).unwrap()
    }

    /// Returns tradeBin1m data
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `tradeBin1m` stream
    pub fn get_trade_bin_1m(&self) -> Vec<TradeBucketed> {
        let data = self.data.read().unwrap();
        let trade_data = data.get("tradeBin1m");
        assert!(trade_data.is_some());
        let trade_data = trade_data.unwrap().clone();
        serde_json::from_value(json!(trade_data)).unwrap()
    }

    /// Returns tradeBin5m data
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `tradeBin5m` stream
    pub fn get_trade_bin_5m(&self) -> Vec<TradeBucketed> {
        let data = self.data.read().unwrap();
        let trade_data = data.get("tradeBin5m");
        assert!(trade_data.is_some());
        let trade_data = trade_data.unwrap().clone();
        serde_json::from_value(json!(trade_data)).unwrap()
    }

    /// Returns tradeBin1h data
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `tradeBin1h` stream
    pub fn get_trade_bin_1h(&self) -> Vec<TradeBucketed> {
        let data = self.data.read().unwrap();
        let trade_data = data.get("tradeBin1h");
        assert!(trade_data.is_some());
        let trade_data = trade_data.unwrap().clone();
        serde_json::from_value(json!(trade_data)).unwrap()
    }

    /// Returns tradeBin1d data
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `tradeBin1d` stream
    pub fn get_trade_bin_1d(&self) -> Vec<TradeBucketed> {
        let data = self.data.read().unwrap();
        let trade_data = data.get("tradeBin1d");
        assert!(trade_data.is_some());
        let trade_data = trade_data.unwrap().clone();
        serde_json::from_value(json!(trade_data)).unwrap()
    }

    /// Returns execution data
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `execution` stream
    pub fn get_execution(&self) -> Vec<Execution> {
        let data = self.data.read().unwrap();
        let execution_data = data.get("execution");
        assert!(execution_data.is_some());
        let execution_data = execution_data.unwrap().clone();
        serde_json::from_value(json!(execution_data)).unwrap()
    }

    /// Returns margin data
    ///
    /// # Panics
    /// Will panic if the user did not subscribe to the `margin` stream
    pub fn get_margin(&self) -> Vec<Margin> {
        let data = self.data.read().unwrap();
        let margin_data = data.get("margin");
        assert!(margin_data.is_some());
        let margin_data = margin_data.unwrap().clone();
        serde_json::from_value(json!(margin_data)).unwrap()
    }
}

/// Handles BitMex websocket 'partial' message.
async fn handle_partial(symbol: &'static str, testnet: bool, auth_data: AuthData<'static>, table: &str, keys: Value, data: Value, data_memory: Data, keys_memory: Keys, l2_ids: L2ids) {
    let data_rec: Vec<Map<String, Value>> = serde_json::from_value(data).unwrap();
    let keys: Vec<String> = serde_json::from_value(keys).unwrap();
    let rest = BitmexRest::new(testnet, auth_data).await;

    keys_memory.write().unwrap().insert(String::from(table), keys);
    if table == "order" {
        let mut filter: HashMap<&str, Value> = HashMap::with_capacity(1);
        filter.insert("open", json!(true));

        let mut params: HashMap<&str, Value> = HashMap::with_capacity(4);
        params.insert("symbol", json!(symbol));
        params.insert("reverse", json!(true));
        params.insert("count", json!(LEN_ORDERS));
        params.insert("filter", json!(filter));

        let response = rest.get_order(params).await;
        assert!(response.is_ok());
        let mut response_vec: Vec<Map<String, Value>> = serde_json::from_value(response.unwrap()).unwrap();
        response_vec.reverse();

        data_memory.write().unwrap().insert(String::from(table), response_vec);
    } else if table.starts_with("tradeBin") {
        let mut params: HashMap<&str, Value> = HashMap::with_capacity(4);
        params.insert("symbol", json!(symbol));
        params.insert("reverse", json!(true));
        params.insert("binSize", json!(table.replace("tradeBin", "")));
        params.insert("count", json!(LEN_TRADE_BUCKETED));

        let response = rest.get_trades_bucketed(params).await;
        assert!(response.is_ok());
        let mut response_vec: Vec<Map<String, Value>> = serde_json::from_value(response.unwrap()).unwrap();
        response_vec.reverse();

        data_memory.write().unwrap().insert(String::from(table), response_vec);
    } else {
        if table == "orderBookL2" {
            let mut l2_ids_w = l2_ids.write().unwrap();
            let ids: Vec<u64> = data_rec.par_iter().map(|x| x.get("id").unwrap().as_u64().unwrap()).collect();
            *l2_ids_w = ids;
        }
        data_memory.write().unwrap().insert(String::from(table), data_rec);
    }
}

/// Handles BitMex websocket `insert`, `update`, `delete` message.
fn handle_data_msg(table: String, action: String, data_rec: Value, data_memory: Data, keys: Keys, l2_ids: L2ids) {
    let mut data_rec: Vec<Map<String, Value>> = serde_json::from_value(data_rec).unwrap();
    let mut data_memory = data_memory.write().unwrap();

    if &action[..] == "insert" {
        let data_memory: &mut Vec<Map<String, Value>> = data_memory.get_mut(&table[..]).unwrap();

        // vec cannot grow forever, need to trim data
        let len = match &table[..] {
            "order" => LEN_ORDERS,
            table if table.contains("orderBookL2") => usize::MAX,
            table if table.contains("tradeBin") => LEN_TRADE_BUCKETED,
            _ => LEN_DEFAULT,
        };

        if data_memory.len() + data_rec.len() > len {
            // rmv_count >=1
            let rmv_count = data_memory.len() + data_rec.len() - len;
            for _ in 1..=rmv_count {
                if data_memory.len() > 0 {
                    data_memory.remove(0);
                }
            }
        }
        // inserts w/ table 'orderBookL2' are not FIFO, they are sorted by price
        if table == "orderBookL2" {
            for map in data_rec {
                let id = map.get("id").unwrap().as_u64().unwrap();
                let location = l2_ids.read().unwrap().binary_search(&id);
                match location {
                    Err(pos) => {
                        l2_ids.write().unwrap().insert(pos, id);
                        data_memory.insert(pos, map);
                    }
                    _ => {}
                }
            }
        } else {
            data_memory.append(&mut data_rec);
        }
    } else {
        for update_data in data_rec {
            let data_memory: &mut Vec<Map<String, Value>> = data_memory.get_mut(&table[..]).unwrap();
            #[allow(unused_assignments)]
            let mut item = None;
            if table == "orderBookL2" {
                item = find_item_order_bookl2(&data_memory, &update_data);
            } else {
                item = find_item_by_keys(keys.read().unwrap().get(&table[..]).unwrap(), &data_memory, &update_data)
            }
            if action == "update" {
                if let Some((index, mut item)) = item {
                    item.extend(update_data.clone());
                    data_memory.remove(index);
                    data_memory.insert(index, item);
                }
                if let Some(timestamp) = update_data.get("timestamp") {
                    let date = DateTime::parse_from_rfc3339(timestamp.as_str().unwrap()).unwrap();
                    let now: DateTime<Utc> = Utc::now();
                    let delay = now.timestamp() - date.timestamp();
                    // if data received is delayed by more than 15 seconds
                    if delay > DELAY_RC {
                        warn!("Received data delayed by {} seconds, table: '{}' action: '{}'", now.timestamp() - date.timestamp(), table, action);
                    }
                }
            } else if action == "delete" {
                if let Some((index, _)) = item {
                    if table == "orderBookL2" {
                        l2_ids.write().unwrap().remove(index);
                    }
                    data_memory.remove(index);
                }
            }
        }
    }
}

/// Tries to connect to BitMex websocket and returns socket, if any error is thrown retries after a timeout
fn connect_ws<'a>(base_uri: &str, symbol: &str, auth_data: &AuthData<'a>, subscriptions: &Vec<Subscriptions>) -> WebSocket<AutoStream> {
    loop {
        if let Ok(ws_stream) = connect(Url::parse(base_uri).unwrap()) {
            let mut socket = ws_stream.0;
            let mut args: Vec<Value> = Vec::with_capacity(subscriptions.len());
            for x in subscriptions {
                let sub = x.value().to_string();
                if sub == "margin" || sub == "wallet" || sub == "transact" || sub.contains("Notifications") || sub == "chat" || sub == "announcement" || sub == "connected" {
                    args.push(json!(sub));
                } else {
                    args.push(json!(sub + ":" + symbol));
                }
            }
            authenticate(&mut socket, auth_data);
            send_command(&mut socket, "subscribe", &args);
            return socket;
        }
        sleep(Duration::from_millis(CONNECT_ERR_SLEEP_MS))
    }
}

/// Send command to BitMex websocket to authenticate account, if authData has API key and secret
fn authenticate<'a>(socket: &mut WebSocket<AutoStream>, auth_data: &AuthData<'a>) {
    match auth_data {
        AuthData::Data { key, secret } => {
            let expires = get_unix_time_secs() + 5;
            let sig = generate_signature(secret, "GET", "/realtime", &expires.to_string()[..], "");
            let mut args: Vec<Value> = Vec::with_capacity(3);
            args.push(json!(key));
            args.push(json!(expires));
            args.push(json!(sig));
            send_command(socket, "authKeyExpires", &args);
        }
        AuthData::None => {}
    }
}

/// Sends raw command.
#[allow(unused_must_use)]
fn send_command(socket: &mut WebSocket<AutoStream>, command: &str, args: &Vec<Value>) {
    let mut params: HashMap<&str, Value> = HashMap::with_capacity(2);
    params.insert("op", json!(command));
    params.insert("args", json!(args));
    let json = serde_json::to_string(&params);
    assert!(json.is_ok());
    socket.write_message(Message::Text(json.unwrap()));
}

fn find_item_by_keys(keys: &Vec<String>, table: &Vec<Map<String, Value>>, match_data: &Map<String, Value>) -> Option<(usize, Map<String, Value>)> {
    for (index, item) in table.iter().enumerate() {
        let mut matched = true;
        for key in keys {
            if item.get(key).unwrap() != match_data.get(key).unwrap() {
                matched = false;
            }
        }
        if matched {
            return Some((index, item.clone()));
        }
    }
    None
}

fn find_item_order_bookl2(table: &Vec<Map<String, Value>>, match_data: &Map<String, Value>) -> Option<(usize, Map<String, Value>)> {
    let id = match_data.get("id").unwrap().as_u64().unwrap();
    let index = table.binary_search_by(|v| {
        v.get("id").unwrap().as_u64().unwrap().partial_cmp(&id).unwrap()
    });
    return match index {
        Ok(pos) => Some((pos, table.get(pos).unwrap().clone())),
        _ => None
    };
}

fn get_unix_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect(ERR_SYS_TIME).as_secs()
}

#[cfg(test)]
mod tests {
    use crate::websocket::BitmexWs;
    use crate::utils::auth::AuthData;
    use serde_json::{Value, Map, json};
    use actix_rt::System;
    use log::{debug, error, log_enabled, info, Level};
    use std::thread;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use std::collections::HashMap;
    use crate::utils::enums::Subscriptions;

    #[test]
    fn test_ws_message_handler() {
        System::new("test").block_on(async {
            env_logger::init();
            let mut sub = Vec::new();
            //sub.push(Subscriptions::Order);
            sub.push(Subscriptions::Instrument);
            sub.push(Subscriptions::OrderBookL2);
            sub.push(Subscriptions::TradeBin5m);
            sub.push(Subscriptions::TradeBin1m);
            sub.push(Subscriptions::Quote);
            //let mut ws = BitmexWs::new(true, "XBTUSD", 1, sub, AuthData::Data { key: "9LLU987YqeeQUbSB4SRB1I3x", secret: "XH-5_Yrm0tBqZxSEVHPY6mG5CMsFCamTlAQa7YNcjZAa639W" }).await;
            let mut ws = BitmexWs::new(false, "XBTUSD", 4, sub, AuthData::None).await;
            loop {
                info!("{:?}", ws.get_instrument().get(0).unwrap().impact_mid_price);
                thread::sleep(Duration::from_secs(5));
            }
        });
    }

    #[test]
    fn test_ws_trade_bin_1m() {
        System::new("test").block_on(async {
            env_logger::init();
            let mut sub = Vec::new();
            sub.push(Subscriptions::TradeBin1m);
            let mut ws = BitmexWs::new(true, "XBTUSD", 1, sub, AuthData::None).await;
            ws.run().await;
            info!("{:?}", ws.get_trade_bin_1m());
        });
    }
}