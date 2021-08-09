use sha2::Sha256;
use std::str;
use hmac::{Hmac, Mac, NewMac};
use hex;

type HmacSha256 = Hmac<Sha256>;

const ERR_MSG: &str = "Error in initializing HMAC-SHA256 instance";

#[derive(Clone, Copy, Debug)]
pub enum AuthData<'a> {
    Data{ key: &'a str, secret: &'a str},
    None,
}

/// Generate a request signature compatible with BitMEX.
pub fn generate_signature(secret: &str, method: &str, path: &str, expires: &str, data: &str) -> String {
    let input_msg = &(method.to_owned() + path + expires + data);
    let mut mac = HmacSha256::new_varkey(secret.as_bytes())
        .expect(ERR_MSG);
    mac.update(input_msg.as_bytes());

    let result = mac.finalize();
    let code_bytes = result.into_bytes();
    hex::encode(&code_bytes)
}

#[cfg(test)]
mod tests {
    use crate::utils::auth::generate_signature;

    #[test]
    fn signature() {
        let sig = generate_signature("chNOOS4KvNXR_Xq4k4c9qsfoKWvnDecLATCRlcBwyKDYnWgO", "POST", "/api/v1/order", "1518064238", "{\"symbol\":\"XBTM15\",\"price\":219.0,\"clOrdID\":\"mm_bitmex_1a/oemUeQ4CAJZgP3fjHsA\",\"orderQty\":98}");
        assert_eq!(sig, "1749cd2ccae4aa49048ae09f0b95110cee706e0944e6a14ad0b3a8cb45bd336b");

        let sig = generate_signature("chNOOS4KvNXR_Xq4k4c9qsfoKWvnDecLATCRlcBwyKDYnWgO", "GET", "/api/v1/instrument?filter=%7B%22symbol%22%3A+%22XBTM15%22%7D", "1518064237", "");
        assert_eq!(sig, "e2f422547eecb5b3cb29ade2127e21b858b235b386bfa45e1c1756eb3383919f");
    }
}

