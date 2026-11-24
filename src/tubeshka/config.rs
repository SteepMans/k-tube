//! Gateway settings, kept apart from upstream's settings file (tubeshka.json in the data folder).
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    /// all traffic through the gateway (on by default); off = upstream behaviour with the user's own proxy
    pub enabled: bool,
    /// the gateway access key, tc_...
    pub token: String,
    /// gateway address when the build has none built in
    pub gateway_url: String,
    /// "stable" or "beta"
    pub channel: String,
}

impl Default for Config {
    fn default() -> Self {
        Self { enabled: true, token: String::new(), gateway_url: String::new(), channel: "stable".into() }
    }
}

fn cell() -> &'static RwLock<Config> {
    static C: OnceLock<RwLock<Config>> = OnceLock::new();
    C.get_or_init(|| RwLock::new(Config::default()))
}

fn path() -> std::path::PathBuf {
    crate::store::data_dir().join("k-tube.json")
}

pub fn load() {
    // the profile of Tubeshka (the previous name) keeps its settings in tubeshka.json
    let old = crate::store::data_dir().join("tubeshka.json");
    if !path().exists() && old.exists() {
        let _ = std::fs::rename(&old, path());
    }
    if let Ok(s) = std::fs::read_to_string(path()) {
        if let Ok(c) = serde_json::from_str::<Config>(&s) {
            *cell().write() = c;
        }
    }
    // a key or address given in the environment (tests, portable runs) wins for this run
    let mut c = cell().write();
    if let Ok(t) = std::env::var("TUBESHKA_TOKEN") {
        c.token = t.trim().to_string();
    }
    if let Ok(u) = std::env::var("TUBESHKA_GATEWAY") {
        c.gateway_url = u.trim().to_string();
    }
}

pub fn get() -> Config {
    cell().read().clone()
}

pub fn update(f: impl FnOnce(&mut Config)) {
    let mut c = cell().write();
    f(&mut c);
    if let Ok(s) = serde_json::to_string_pretty(&*c) {
        let _ = std::fs::write(path(), s);
    }
}

/// The gateway's base URL (https://host), from the build or the settings. The form wss://host/v1/stream is accepted
/// too: only the host is taken.
pub fn gateway_url() -> Option<String> {
    let built = super::built_in_gateway();
    let u = if built.is_empty() { get().gateway_url } else { built.to_string() };
    normalize(&u)
}

fn normalize(u: &str) -> Option<String> {
    let u = u.trim();
    let rest = u.strip_prefix("https://").or_else(|| u.strip_prefix("wss://"))?;
    let host = rest.split(['/', '?', '#']).next()?.to_ascii_lowercase();
    (!host.is_empty() && host.contains('.') && !host.contains('@')).then(|| format!("https://{host}"))
}

/// Host of the gateway (for the direct route of the local proxy).
pub fn gateway_host() -> Option<String> {
    let u = gateway_url()?;
    let rest = u.strip_prefix("https://")?;
    Some(rest.split(['/', ':']).next()?.to_ascii_lowercase())
}

/// Whether a key looks like a gateway access key.
pub fn valid_token(t: &str) -> bool {
    t.len() == 46 && t.starts_with("tc_") && t[3..].bytes().all(|b| b.is_ascii_alphanumeric())
}

/// Scheme of the key links the gateway hands out (gateway protocol, kept as is so existing links keep working).
const KEY_LINK_PREFIX: &str = "tgeshka://gateway?";

/// Parses a key link <scheme>://gateway?url=...&token=... (sent together with a key).
pub fn parse_link(s: &str) -> Option<(Option<String>, Option<String>)> {
    let q = s.trim().strip_prefix(KEY_LINK_PREFIX)?;
    let mut url = None;
    let mut token = None;
    for kv in q.split('&') {
        let (k, v) = kv.split_once('=')?;
        let v = crate::api::http::urldecode(v);
        match k {
            "url" => url = Some(v),
            "token" => token = Some(v),
            _ => {}
        }
    }
    Some((url, token))
}

#[cfg(test)]
mod tests {
    #[test]
    fn link_and_token() {
        let t = format!("tc_{}", "a".repeat(43));
        assert!(super::valid_token(&t));
        assert!(!super::valid_token("tc_short"));
        let (u, k) = super::parse_link(&format!("{}url=https%3A%2F%2Fgw.example&token={t}", super::KEY_LINK_PREFIX)).unwrap();
        assert_eq!(u.as_deref(), Some("https://gw.example"));
        assert_eq!(k.as_deref(), Some(t.as_str()));
        assert_eq!(super::normalize("wss://gw.example/v1/stream").as_deref(), Some("https://gw.example"));
        assert_eq!(super::normalize("https://GW.example/").as_deref(), Some("https://gw.example"));
        assert_eq!(super::normalize("http://gw.example"), None);
    }
}
