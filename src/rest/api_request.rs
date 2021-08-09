use std::collections::HashMap;
use crate::utils::enums::HttpMethod;
use serde_json::Value;

pub struct ApiRequest<'a> {
    endpoint: &'a str,
    method: HttpMethod,
    query: HashMap<&'a str, Value>,
    body: HashMap<&'a str, Value>,
}

impl<'a> ApiRequest<'a> {
    pub fn new(endpoint: &'a str, method: HttpMethod, query: HashMap<&'a str, Value>,  body: HashMap<&'a str, Value>) -> ApiRequest<'a> {
        ApiRequest {
            endpoint,
            method,
            query,
            body,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn method(&self) -> &HttpMethod { &self.method }

    pub fn query(&self) -> &HashMap<&'a str, Value> {
        &self.query
    }

    pub fn body(&self) -> &HashMap<&'a str, Value> {
        &self.body
    }
}