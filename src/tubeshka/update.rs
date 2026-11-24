//! Updates through the gateway (docs/update-v1.md in api-gateway): the manifest and the installer come only from the
//! gateway with the key, the installer runs only if its sha256 is signed by our ed25519 key (over the raw 32 bytes).
use anyhow::{Result, anyhow};
use base64::Engine as _;
use serde_json::Value;

/// Public half of the release signing key (the private one is the CI secret TUBESHKA_UPDATE_KEY).
pub const PUBLIC_KEY_B64: &str = "3fPRuJlf7InepS2UjU5FbQm+rVN+LEwKepqyYGbk0W0=";

#[derive(Clone, Debug)]
pub struct Manifest {
    pub version: String,
    pub min_version: String,
    pub sha256: String,
    pub size: u64,
    pub signature: String,
    pub notes: String,
    /// /v1/update/package/streamtube/<version>
    pub path: String,
}

pub fn channel() -> String {
    let c = super::config::get().channel;
    if c == "beta" { c } else { "stable".into() }
}

/// The newest release of our channel, whatever its version (the caller compares).
pub fn latest() -> Result<Option<Manifest>> {
    let Some(v) = super::gateway::manifest(&channel())? else { return Ok(None) };
    let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or_default().to_string();
    let m = Manifest {
        version: s("version"),
        min_version: s("min_version"),
        sha256: s("sha256").to_ascii_lowercase(),
        size: v.get("size").and_then(Value::as_u64).unwrap_or(0),
        signature: s("signature"),
        notes: s("notes"),
        path: s("url"),
    };
    if m.version.is_empty() || m.sha256.len() != 64 || !m.path.starts_with("/v1/update/package/streamtube/") {
        return Err(anyhow!("шлюз отдал неполный манифест"));
    }
    Ok(Some(m))
}

/// Checks the ed25519 signature over the raw sha256 digest (hex).
pub fn verify(sha256_hex: &str, signature_b64: &str) -> bool {
    let Ok(key) = base64::engine::general_purpose::STANDARD.decode(PUBLIC_KEY_B64) else { return false };
    let Ok(sig) = base64::engine::general_purpose::STANDARD.decode(signature_b64.trim()) else { return false };
    let digest: Option<Vec<u8>> =
        (0..sha256_hex.len()).step_by(2).map(|i| sha256_hex.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok())).collect();
    let Some(digest) = digest.filter(|d| d.len() == 32) else { return false };
    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, key).verify(&digest, &sig).is_ok()
}

#[cfg(test)]
mod tests {
    #[test]
    fn release_signature_verifies() {
        // sha256("tubeshka") signed with the release key (openssl pkeyutl -sign -rawin over the raw digest)
        let sha = "36da92de03416244aa22c776b5a978406d975b3790945c354120d04aee2e08bc";
        let sig = "b8W0TJjcmB6TtKaI5nXGIbaW22O9egglQvAnO3vAviFzKEBkMxB+jH6IhLvCC7KgGP2e7AGZZmvKT29O4VCwBw==";
        assert!(super::verify(sha, sig));
        assert!(!super::verify(&sha.replace("36", "37"), sig));
    }

    #[test]
    fn bad_signatures_fail() {
        let sha = "ab".repeat(32);
        assert!(!super::verify(&sha, ""));
        assert!(!super::verify(&sha, &base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 64])));
        assert!(!super::verify("zz", "AAAA"));
    }
}
