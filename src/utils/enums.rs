use serde_json::Value;
use awc::error::SendRequestError;

pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete
}

impl<'a> HttpMethod {
    pub fn value(&self) -> &'a str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
        }
    }
}

#[derive(Debug)]
pub enum RequestResponseErr {
    Data(Value),
    SendRequestError(SendRequestError),
}

#[derive(Clone)]
pub enum Subscriptions {
    Announcement,
    // Site announcements
    Chat,
    // Trollbox chat
    Connected,
    // Statistics of connected users/bots
    Funding,
    // Updates of swap funding rates. Sent every funding interval (usually 8hrs)
    Instrument,
    // Instrument updates including turnover and bid/ask
    Insurance,
    // Daily Insurance Fund updates
    Liquidation,
    // Liquidation orders as they're entered into the book
    OrderBookL225,
    // Top 25 levels of level 2 order book
    OrderBookL2,
    // Full level 2 order book
    OrderBook10,
    // Top 10 levels using traditional full book push
    PublicNotifications,
    // System-wide notifications (used for short-lived messages)
    Quote,
    // Top level of the book
    QuoteBin1m,
    // 1-minute quote bins
    QuoteBin5m,
    // 5-minute quote bins
    QuoteBin1h,
    // 1-hour quote bins
    QuoteBin1d,
    // 1-day quote bins
    Settlement,
    // Settlements
    Trade,
    // Live trades
    TradeBin1m,
    // 1-minute trade bins
    TradeBin5m,
    // 5-minute trade bins
    TradeBin1h,
    // 1-hour trade bins
    TradeBin1d,
    // 1-day trade bins
    Affiliate,
    // Affiliate status, such as total referred users & payout %
    Execution,
    // Individual executions; can be multiple per order
    Order,
    // Live updates on your orders
    Margin,
    // Updates on your current account balance and margin requirements
    Position,
    // Updates on your positions
    PrivateNotifications,
    // Individual notifications - currently not used
    Transact,
    // Deposit/Withdrawal updates
    Wallet,
}

impl<'a> Subscriptions {
    pub fn value(&self) -> &'a str {
        match self {
            Subscriptions::Instrument => "instrument",
            Subscriptions::Announcement => "announcement",
            Subscriptions::Chat => "chat",
            Subscriptions::Connected => "connected",
            Subscriptions::Funding => "funding",
            Subscriptions::Insurance => "insurance",
            Subscriptions::Liquidation => "liquidation",
            Subscriptions::OrderBookL225 => "orderBookL2_25",
            Subscriptions::OrderBookL2 => "orderBookL2",
            Subscriptions::OrderBook10 => "orderBook10",
            Subscriptions::PublicNotifications => "publicNotifications",
            Subscriptions::Quote => "quote",
            Subscriptions::QuoteBin1m => "quoteBin1m",
            Subscriptions::QuoteBin5m => "quoteBin5m",
            Subscriptions::QuoteBin1h => "quoteBin1h",
            Subscriptions::QuoteBin1d => "quoteBin1d",
            Subscriptions::Settlement => "settlement",
            Subscriptions::Trade => "trade",
            Subscriptions::TradeBin1m => "tradeBin1m",
            Subscriptions::TradeBin5m => "tradeBin5m",
            Subscriptions::TradeBin1h => "tradeBin1h",
            Subscriptions::TradeBin1d => "tradeBin1d",
            Subscriptions::Affiliate => "affiliate",
            Subscriptions::Execution => "execution",
            Subscriptions::Order => "order",
            Subscriptions::Margin => "margin",
            Subscriptions::Position => "position",
            Subscriptions::PrivateNotifications => "privateNotifications",
            Subscriptions::Transact => "transact",
            Subscriptions::Wallet => "wallet",
        }
    }
}