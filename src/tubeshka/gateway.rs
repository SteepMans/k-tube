//! Talking to the gateway: proxy connections (async, for the local proxy) and its small HTTP API (/v1/info,
//! /v1/update/*), which goes straight to the gateway, never through the local proxy.
use super::config;
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

pub struct OpenError {
    pub status: u16,
    pub text: String,
}

fn err(status: u16, text: impl Into<String>) -> OpenError {
    OpenError { status, text: text.into() }
}

/// Opens host:port through the gateway and returns the stream after 101 (raw bytes to the site from then on).
pub async fn open_tunnel(host: &str, port: u16) -> Result<TlsStream<TcpStream>, OpenError> {
    let c = config::get();
    if !config::valid_token(&c.token) {
        return Err(err(407, "нет ключа шлюза"));
    }
    let Some(gw_host) = config::gateway_host() else { return Err(err(502, "не задан адрес шлюза")) };
    let tcp = tokio::time::timeout(Duration::from_secs(15), TcpStream::connect((gw_host.as_str(), 443)))
        .await
        .map_err(|_| err(502, "шлюз не отвечает"))?
        .map_err(|e| err(502, format!("шлюз недоступен: {e}")))?;
    tcp.set_nodelay(true).ok();
    let name = rustls::pki_types::ServerName::try_from(gw_host.clone()).map_err(|_| err(502, "неверный адрес шлюза"))?;
    let mut tls = tokio::time::timeout(Duration::from_secs(15), tokio_rustls::TlsConnector::from(super::proxy::tls()).connect(name, tcp))
        .await
        .map_err(|_| err(502, "TLS со шлюзом: таймаут"))?
        .map_err(|e| err(502, format!("TLS со шлюзом: {e}")))?;
    let req = format!(
        "GET /v1/proxy HTTP/1.1\r\nHost: {gw_host}\r\nConnection: Upgrade\r\nUpgrade: tcp/1\r\nAuthorization: Bearer {}\r\n\
         {}: {}\r\nX-Proxy-Target: {host}:{port}\r\n\r\n",
        c.token,
        super::CLIENT_HEADER,
        super::client_header()
    );
    tls.write_all(req.as_bytes()).await.map_err(|e| err(502, format!("шлюз: {e}")))?;
    // read the answer head byte by byte: nothing after it may be consumed (the site's bytes follow at once)
    let mut head = Vec::with_capacity(256);
    let mut b = [0u8; 1];
    let read = async {
        loop {
            let n = tls.read(&mut b).await?;
            if n == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }
            head.push(b[0]);
            if head.ends_with(b"\r\n\r\n") || head.len() > 8192 {
                return Ok(());
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), read)
        .await
        .map_err(|_| err(502, "шлюз не ответил"))?
        .map_err(|e| err(502, format!("шлюз: {e}")))?;
    let text = String::from_utf8_lossy(&head);
    let status: u16 = text.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    if status == 101 {
        return Ok(tls);
    }
    // the body is a short JSON error; read what has come
    let mut body = vec![0u8; 256];
    let n = tokio::time::timeout(Duration::from_millis(500), tls.read(&mut body)).await.ok().and_then(|r| r.ok()).unwrap_or(0);
    let detail = String::from_utf8_lossy(&body[..n]).trim().to_string();
    Err(match status {
        404 => err(404, "ключ не принят шлюзом (отозван или опечатка)"),
        403 => err(403, format!("шлюз не пускает к {host}")),
        429 => err(429, "слишком много соединений по ключу"),
        503 if text.to_ascii_lowercase().contains("x-proxy-error: no_leg") => {
            err(503, "выходы шлюза для YouTube сейчас недоступны, попробуйте через несколько минут")
        }
        _ => err(status, format!("шлюз ответил {status} {detail}")),
    })
}

/// Agent for the gateway's API: direct (the gateway is not behind the local proxy), browser-independent roots.
fn agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_connect(Some(Duration::from_secs(15)))
            .timeout_recv_response(Some(Duration::from_secs(30)))
            .proxy(None)
            .build()
            .new_agent()
    })
}

fn base() -> Result<(String, String)> {
    let c = config::get();
    if !config::valid_token(&c.token) {
        return Err(anyhow!("нет ключа шлюза"));
    }
    let url = config::gateway_url().ok_or_else(|| anyhow!("не задан адрес шлюза"))?;
    Ok((url, c.token))
}

fn get(path: &str) -> Result<(u16, Option<ureq::Body>)> {
    let (url, token) = base()?;
    let resp = agent()
        .get(&format!("{url}{path}"))
        .header("Authorization", &format!("Bearer {token}"))
        .header(super::CLIENT_HEADER, &super::client_header())
        .call()?;
    let status = resp.status().as_u16();
    Ok((status, Some(resp.into_body())))
}

/// State of the key: /v1/info and how long it took.
#[derive(Clone, Debug)]
pub struct Info {
    pub description: String,
    pub id: i64,
    pub rtt_ms: u128,
    pub proxy: bool,
    pub upgrade_required: bool,
}

pub fn info() -> Result<Info> {
    let t = std::time::Instant::now();
    let (status, body) = get("/v1/info")?;
    if status == 404 {
        return Err(anyhow!("ключ не принят шлюзом (отозван или опечатка)"));
    }
    if status != 200 {
        return Err(anyhow!("шлюз ответил {status}"));
    }
    let v: Value = serde_json::from_str(&body.ok_or_else(|| anyhow!("пустой ответ"))?.read_to_string()?)?;
    Ok(Info {
        description: v.get("description").and_then(Value::as_str).unwrap_or("").to_string(),
        id: v.get("id").and_then(Value::as_i64).unwrap_or(0),
        rtt_ms: t.elapsed().as_millis(),
        proxy: v.get("proxy").and_then(Value::as_bool).unwrap_or(false),
        upgrade_required: v.get("upgrade_required").and_then(Value::as_bool).unwrap_or(false),
    })
}

/// The newest release for our platform and channel (None: nothing published).
pub fn manifest(channel: &str) -> Result<Option<Value>> {
    let (status, body) = get(&format!("/v1/update/manifest?platform=streamtube&channel={channel}"))?;
    match status {
        200 => Ok(Some(serde_json::from_str(&body.ok_or_else(|| anyhow!("пустой ответ"))?.read_to_string()?)?)),
        204 => Ok(None),
        404 => Err(anyhow!("ключ не принят шлюзом")),
        s => Err(anyhow!("шлюз ответил {s}")),
    }
}

/// Streams a package (path from the manifest, /v1/update/package/...) into `out`.
pub fn download(path: &str, out: &mut dyn std::io::Write, mut progress: impl FnMut(u64)) -> Result<()> {
    use std::io::Read;
    if !path.starts_with("/v1/update/package/") {
        return Err(anyhow!("неожиданный путь пакета"));
    }
    let (url, token) = base()?;
    let resp = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_connect(Some(Duration::from_secs(15)))
        .proxy(None)
        .build()
        .new_agent()
        .get(&format!("{url}{path}"))
        .header("Authorization", &format!("Bearer {token}"))
        .header(super::CLIENT_HEADER, &super::client_header())
        .call()?;
    if resp.status().as_u16() != 200 {
        return Err(anyhow!("шлюз ответил {}", resp.status()));
    }
    let mut r = resp.into_body().into_with_config().limit(u64::MAX).reader();
    let mut buf = vec![0u8; 256 * 1024];
    let mut done = 0u64;
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            return Ok(());
        }
        out.write_all(&buf[..n])?;
        done += n as u64;
        progress(done);
    }
}
