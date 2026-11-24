//! K-Tube: this fork's additions. All network traffic of the app goes through an external proxy gateway: a local
//! HTTP proxy hands every connection to the gateway, which reaches YouTube from its own exits. The gateway's address
//! comes from the build (an environment variable in CI) or from the settings, the access key from the user.
pub mod config;
pub mod gateway;
pub mod proxy;
pub mod ui;
pub mod update;

/// Shown name of the app.
pub const APP_NAME: &str = "K-Tube";
/// Our repository (release page, "What's new").
pub const REPO_URL: &str = "https://github.com/SteepMans/k-tube";

/// Version of this fork (tubeshka-version.txt): updates and the gateway's statistics use it, not the upstream version.
/// A release build gets its version from the tag (TUBESHKA_VERSION), other builds from the file.
pub fn version() -> &'static str {
    match option_env!("TUBESHKA_VERSION") {
        Some(v) if !v.trim().is_empty() => v.trim(),
        _ => include_str!("../../tubeshka-version.txt").trim(),
    }
}

/// Gateway address built in by CI (secret), empty in local builds without it: the key screen then asks for it.
pub fn built_in_gateway() -> &'static str {
    option_env!("TUBESHKA_GATEWAY_URL").unwrap_or("").trim()
}

/// Header that names the client to the gateway (gateway protocol: the server expects exactly this name).
pub const CLIENT_HEADER: &str = "X-TGeshka-Client";

/// "streamtube/<version>": the client name the gateway counts.
pub fn client_header() -> String {
    format!("streamtube/{}", version())
}

/// One entry of CHANGELOG-tubeshka.json ("What's new").
#[derive(serde::Deserialize, Clone, Debug)]
pub struct Change {
    pub version: String,
    pub date: String,
    pub items: Vec<String>,
}

pub fn changelog() -> Vec<Change> {
    serde_json::from_str(include_str!("../../CHANGELOG-tubeshka.json")).unwrap_or_default()
}

/// Starts the local proxy; call before anything reads the proxy setting.
pub fn init() {
    config::load();
    proxy::start();
}

#[cfg(test)]
mod tests {
    #[test]
    fn changelog_parses() {
        let c = super::changelog();
        assert!(!c.is_empty(), "CHANGELOG-tubeshka.json is empty or broken");
        assert!(c.iter().all(|e| !e.items.is_empty()));
    }
}
