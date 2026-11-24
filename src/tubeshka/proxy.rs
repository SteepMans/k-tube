//! Local HTTP proxy on 127.0.0.1: every client of the app (ureq, libmpv, yt-dlp, the sign-in browser) is pointed at
//! it. Each connection to a YouTube/Google host is opened on the gateway (GET /v1/proxy, Upgrade: tcp/1, the key)
//! and the leg abroad resolves and connects the name: nothing about these hosts is resolved or dialed from here.
//! The gateway itself and GitHub (yt-dlp and FFmpeg downloads) go direct; anything else is refused.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Hosts the gateway lets through (the same list as the server, internal/proxyhosts in api-gateway).
const GATEWAY_HOSTS: &[&str] = &[
    "youtube.com", "*.youtube.com", "youtu.be", "youtube-nocookie.com", "*.youtube-nocookie.com", "*.googlevideo.com",
    "*.ytimg.com", "*.ggpht.com", "google.com", "*.google.com", "*.gstatic.com", "*.googleapis.com",
    "*.googleusercontent.com", "sponsor.ajay.app", "dearrow-thumb.ajay.app", "returnyoutubedislikeapi.com",
];
/// Hosts reached directly: tool downloads from GitHub (not YouTube traffic, open from Russia).
const DIRECT_HOSTS: &[&str] = &["github.com", "api.github.com", "objects.githubusercontent.com", "release-assets.githubusercontent.com"];

#[derive(Default)]
pub struct Stats {
    pub active: AtomicU64,
    pub opened: AtomicU64,
    pub failed: AtomicU64,
    pub refused: AtomicU64,
    pub down: AtomicU64,
    pub up: AtomicU64,
}

pub fn stats() -> &'static Stats {
    static S: OnceLock<Stats> = OnceLock::new();
    S.get_or_init(Stats::default)
}

static LAST_ERROR: parking_lot::Mutex<Option<(std::time::Instant, String)>> = parking_lot::Mutex::new(None);

/// The last refusal of the gateway within 2 minutes (shown in the settings).
pub fn last_error() -> Option<String> {
    LAST_ERROR.lock().as_ref().filter(|(t, _)| t.elapsed() < Duration::from_secs(120)).map(|(_, e)| e.clone())
}

fn set_error(e: String) {
    if crate::perf::on() {
        eprintln!("[gateway] {e}");
    }
    *LAST_ERROR.lock() = Some((std::time::Instant::now(), e));
}

static PORT: OnceLock<u16> = OnceLock::new();

/// http://127.0.0.1:<port> of the local proxy, when it runs.
pub fn local_url() -> Option<String> {
    PORT.get().map(|p| format!("http://127.0.0.1:{p}"))
}

fn matches(list: &[&str], host: &str) -> bool {
    list.iter().any(|p| match p.strip_prefix("*.") {
        Some(sfx) => host.len() > sfx.len() + 1 && host.ends_with(sfx) && host.as_bytes()[host.len() - sfx.len() - 1] == b'.',
        None => host == *p,
    })
}

#[derive(Debug, PartialEq)]
enum Route {
    Gateway,
    Direct,
    Refuse,
}

fn route(host: &str) -> Route {
    if crate::tubeshka::config::gateway_host().as_deref() == Some(host) || matches(DIRECT_HOSTS, host) {
        Route::Direct
    } else if matches(GATEWAY_HOSTS, host) {
        Route::Gateway
    } else {
        Route::Refuse
    }
}

pub fn start() {
    if PORT.get().is_some() {
        return;
    }
    let listener = match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(l) => l,
        Err(e) => {
            set_error(format!("local proxy: {e}"));
            return;
        }
    };
    let Ok(port) = listener.local_addr().map(|a| a.port()) else { return };
    let _ = listener.set_nonblocking(true);
    let _ = PORT.set(port);
    std::thread::Builder::new()
        .name("gateway-proxy".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("gateway-io").enable_all().build();
            let Ok(rt) = rt else { return };
            rt.block_on(async move {
                let Ok(l) = TcpListener::from_std(listener) else { return };
                loop {
                    if let Ok((c, _)) = l.accept().await {
                        tokio::spawn(async move {
                            let _ = handle(c).await;
                        });
                    }
                }
            });
        })
        .ok();
}

/// Reads the request head (up to the blank line); returns it and the bytes after it.
async fn read_head(c: &mut TcpStream) -> std::io::Result<(String, Vec<u8>)> {
    let mut buf = Vec::with_capacity(2048);
    let mut tmp = [0u8; 2048];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(30), c.read(&mut tmp)).await.map_err(|_| std::io::ErrorKind::TimedOut)??;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let rest = buf.split_off(i + 4);
            return Ok((String::from_utf8_lossy(&buf).into_owned(), rest));
        }
        if buf.len() > 64 * 1024 {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
    }
}

fn split_host_port(s: &str, default: u16) -> Option<(String, u16)> {
    let (h, p) = match s.rsplit_once(':') {
        Some((h, p)) if !h.contains(']') || h.ends_with(']') => (h, p.parse().ok()?),
        _ => (s, default),
    };
    let h = h.trim_matches(['[', ']']).trim_end_matches('.').to_ascii_lowercase();
    (!h.is_empty()).then_some((h, p))
}

async fn reply(c: &mut TcpStream, status: &str) {
    let _ = c.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await;
}

async fn handle(mut c: TcpStream) -> std::io::Result<()> {
    c.set_nodelay(true).ok();
    let (head, rest) = read_head(&mut c).await?;
    let mut lines = head.split("\r\n");
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let (method, target, ver) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""), parts.next().unwrap_or("HTTP/1.1"));
    // CONNECT host:port, or a plain-HTTP request in absolute form (http://host/path)
    let (host, port, first_bytes, connect) = if method.eq_ignore_ascii_case("CONNECT") {
        let Some((h, p)) = split_host_port(target, 443) else { return Ok(reply(&mut c, "400 Bad Request").await) };
        (h, p, rest, true)
    } else if let Some(u) = target.strip_prefix("http://") {
        let (auth, path) = u.split_once('/').map(|(a, p)| (a, format!("/{p}"))).unwrap_or((u, "/".into()));
        let Some((h, p)) = split_host_port(auth, 80) else { return Ok(reply(&mut c, "400 Bad Request").await) };
        let mut out = format!("{method} {path} {ver}\r\n");
        for l in lines.filter(|l| !l.is_empty()) {
            let k = l.split(':').next().unwrap_or("").trim().to_ascii_lowercase();
            if k != "proxy-connection" && k != "proxy-authorization" {
                out.push_str(l);
                out.push_str("\r\n");
            }
        }
        out.push_str("\r\n");
        let mut b = out.into_bytes();
        b.extend_from_slice(&rest);
        (h, p, b, false)
    } else {
        return Ok(reply(&mut c, "405 Method Not Allowed").await);
    };
    let st = stats();
    match route(&host) {
        Route::Refuse => {
            st.refused.fetch_add(1, Ordering::Relaxed);
            if crate::perf::on() {
                eprintln!("[gateway] refused {host}");
            }
            reply(&mut c, "403 Forbidden").await;
            Ok(())
        }
        Route::Direct => {
            let up = match tokio::time::timeout(Duration::from_secs(15), TcpStream::connect((host.as_str(), port))).await {
                Ok(Ok(s)) => s,
                _ => return Ok(reply(&mut c, "502 Bad Gateway").await),
            };
            pump(c, up, first_bytes, connect).await
        }
        Route::Gateway => match super::gateway::open_tunnel(&host, port).await {
            Ok(up) => {
                st.opened.fetch_add(1, Ordering::Relaxed);
                pump(c, up, first_bytes, connect).await
            }
            Err(e) => {
                st.failed.fetch_add(1, Ordering::Relaxed);
                let status = match e.status {
                    403 => "403 Forbidden",
                    407 | 404 => "407 Proxy Authentication Required",
                    429 => "429 Too Many Requests",
                    _ => "502 Bad Gateway",
                };
                set_error(format!("{host}: {}", e.text));
                reply(&mut c, status).await;
                Ok(())
            }
        },
    }
}

async fn pump<U>(mut c: TcpStream, mut up: U, first: Vec<u8>, connect: bool) -> std::io::Result<()>
where
    U: AsyncRead + AsyncWrite + Unpin,
{
    let st = stats();
    st.active.fetch_add(1, Ordering::Relaxed);
    let res = async {
        if connect {
            c.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n").await?;
        }
        if !first.is_empty() {
            up.write_all(&first).await?;
        }
        let (a, b) = tokio::io::copy_bidirectional_with_sizes(&mut c, &mut up, 64 << 10, 64 << 10).await?;
        st.up.fetch_add(a + first.len() as u64, Ordering::Relaxed);
        st.down.fetch_add(b, Ordering::Relaxed);
        Ok(())
    }
    .await;
    st.active.fetch_sub(1, Ordering::Relaxed);
    res
}

/// Shared TLS settings for the gateway (system-independent roots, HTTP/1.1 only: Upgrade has no HTTP/2 form).
pub(crate) fn tls() -> Arc<rustls::ClientConfig> {
    static T: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    T.get_or_init(|| {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        let mut c = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("tls versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        c.alpn_protocols = vec![b"http/1.1".to_vec()];
        Arc::new(c)
    })
    .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn routes() {
        assert_eq!(route("rr1---sn-abc.googlevideo.com"), Route::Gateway);
        assert_eq!(route("www.youtube.com"), Route::Gateway);
        assert_eq!(route("accounts.google.com"), Route::Gateway);
        assert_eq!(route("sponsor.ajay.app"), Route::Gateway);
        assert_eq!(route("github.com"), Route::Direct);
        assert_eq!(route("evilyoutube.com"), Route::Refuse);
        assert_eq!(route("googlevideo.com"), Route::Refuse);
        assert_eq!(route("example.com"), Route::Refuse);
        assert_eq!(split_host_port("WWW.YouTube.com.:443", 80), Some(("www.youtube.com".into(), 443)));
        assert_eq!(split_host_port("i.ytimg.com", 80), Some(("i.ytimg.com".into(), 80)));
    }
}
