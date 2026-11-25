//! Local streaming proxy for googlevideo URLs.
//!
//! YouTube refuses open-ended range requests on stream URLs (HTTP 403) and
//! throttles large single requests. mpv asks for whole files, so it talks to
//! this proxy instead, which serves mpv's (possibly open-ended) ranges by
//! fetching bounded chunks from YouTube, like yt-dlp's chunked downloader.
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use parking_lot::Condvar;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

const FIRST_CHUNK: u64 = 1 << 20; // small first chunk: fast start and seeks
const CHUNK: u64 = 8 << 20;

struct Entry {
    url: String,
    len: Option<u64>,
    mime: String,
    /// set for hover previews: (cache key, preview generation it belongs to)
    preview: Option<(String, u64)>,
}

/// Generation of hover previews: bumped when a preview ends or another starts, so
/// the fetches of the previous one stop at once and free their connections.
static PREVIEW_GEN: AtomicU64 = AtomicU64::new(0);

/// Ends the fetches of the current hover preview; returns the new generation.
pub fn preview_next() -> u64 {
    PREVIEW_GEN.fetch_add(1, Ordering::SeqCst) + 1
}

/// Bytes kept per previewed stream (from its start) and for all of them together.
const PREVIEW_ENTRY_MAX: usize = 6 << 20;
const PREVIEW_CACHE_MAX: usize = 64 << 20;

/// The beginnings of recently previewed streams, so hovering the same video again
/// plays from memory instead of downloading it again. Least recently used goes first.
#[derive(Default)]
struct PreviewCache {
    map: HashMap<String, (Vec<u8>, u64)>,
    tick: u64,
    total: usize,
}

fn preview_cache() -> &'static Mutex<PreviewCache> {
    static C: OnceLock<Mutex<PreviewCache>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(PreviewCache::default()))
}

impl PreviewCache {
    /// Cached bytes of `key` in start..=end (empty when `start` is past what is cached).
    fn slice(&mut self, key: &str, start: u64, end: u64) -> Vec<u8> {
        self.tick += 1;
        let tick = self.tick;
        match self.map.get_mut(key) {
            Some((data, used)) if (start as usize) < data.len() => {
                *used = tick;
                let to = (end as usize).saturating_add(1).min(data.len());
                data[start as usize..to].to_vec()
            }
            _ => Vec::new(),
        }
    }

    /// Adds bytes fetched at offset `at`; only a continuation of what is cached is kept.
    fn append(&mut self, key: &str, at: u64, bytes: &[u8]) {
        let have = self.map.get(key).map_or(0, |(d, _)| d.len());
        if at as usize != have || have >= PREVIEW_ENTRY_MAX {
            return;
        }
        let take = bytes.len().min(PREVIEW_ENTRY_MAX - have);
        self.tick += 1;
        let tick = self.tick;
        let e = self.map.entry(key.to_string()).or_insert_with(|| (Vec::new(), tick));
        e.0.extend_from_slice(&bytes[..take]);
        e.1 = tick;
        self.total += take;
        while self.total > PREVIEW_CACHE_MAX {
            let oldest = self.map.iter().filter(|(k, _)| k.as_str() != key).min_by_key(|(_, (_, u))| *u).map(|(k, _)| k.clone());
            let Some(oldest) = oldest else { break };
            if let Some((d, _)) = self.map.remove(&oldest) {
                self.total -= d.len();
            }
        }
    }
}

struct Proxy {
    port: u16,
    streams: Mutex<HashMap<u64, Entry>>,
    /// (url, kind) -> id of plain entries, so a live playlist reloaded every few
    /// seconds gets the same ids for the segments it lists again
    ids: Mutex<HashMap<(String, String), u64>>,
    next: Mutex<u64>,
}

fn proxy() -> Option<&'static Proxy> {
    static P: OnceLock<Option<Proxy>> = OnceLock::new();
    P.get_or_init(|| {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let port = listener.local_addr().ok()?.port();
        std::thread::Builder::new()
            .name("stream-proxy".into())
            .spawn(move || {
                for conn in listener.incoming().flatten() {
                    std::thread::spawn(move || {
                        let _ = serve(conn);
                    });
                }
            })
            .ok()?;
        Some(Proxy { port, streams: Mutex::new(HashMap::new()), ids: Mutex::new(HashMap::new()), next: Mutex::new(0) })
    })
    .as_ref()
}

/// Marks entries served as HLS playlists / plain pass-through instead of ranged media.
const HLS: &str = "#hls";
const PASS: &str = "#pass";

fn register(p: &Proxy, url: &str, len: Option<u64>, mime: &str) -> u64 {
    register_entry(p, url, len, mime, None)
}

fn register_entry(p: &Proxy, url: &str, len: Option<u64>, mime: &str, preview: Option<(String, u64)>) -> u64 {
    // the same URL again (a live playlist lists its segments on every reload): same id
    let key = (url.to_string(), mime.to_string());
    let mut s = p.streams.lock();
    let mut ids = p.ids.lock();
    if preview.is_none() {
        if let Some(id) = ids.get(&key).copied().filter(|id| s.contains_key(id)) {
            return id;
        }
    }
    let id = {
        let mut n = p.next.lock();
        *n += 1;
        *n
    };
    // keep the table bounded: old entries belong to videos no longer playing.
    // HLS registers one entry per segment, so the window is generous. Playlists
    // stay: a live one is reloaded by the same id for as long as it plays.
    if s.len() > 20_000 {
        let min_keep = id.saturating_sub(10_000);
        s.retain(|k, e| *k >= min_keep || e.mime == HLS);
        ids.retain(|_, id| s.contains_key(id));
    }
    s.insert(id, Entry { url: url.to_string(), len, mime: mime.to_string(), preview: preview.clone() });
    if preview.is_none() {
        ids.insert(key, id);
    }
    id
}

/// Returns a local URL that serves `url` to mpv. Non-googlevideo URLs pass through.
pub fn local_url(url: &str, len: Option<u64>, mime: &str) -> String {
    if !url.contains("googlevideo.com") {
        return url.to_string();
    }
    let Some(p) = proxy() else { return url.to_string() };
    let id = register(p, url, len, mime);
    format!("http://127.0.0.1:{}/s/{id}", p.port)
}

/// Like `local_url`, for a hover preview: its beginning is kept in memory under `key`
/// (the same video and format give the same key), and its fetches stop when the
/// preview generation `generation` is over (see `preview_next`).
pub fn preview_url(url: &str, len: Option<u64>, mime: &str, key: &str, generation: u64) -> String {
    if !url.contains("googlevideo.com") {
        return url.to_string();
    }
    let Some(p) = proxy() else { return url.to_string() };
    let id = register_entry(p, url, len, mime, Some((key.to_string(), generation)));
    format!("http://127.0.0.1:{}/s/{id}", p.port)
}

/// Serves an HLS master playlist to mpv through the proxy. Every playlist and
/// segment is fetched by the app (so it uses the app's network settings, e.g.
/// IPv4 only, which the stream URLs are bound to), and variants taller than
/// `max_height` (0 = no limit) are left out so mpv plays the chosen quality.
pub fn local_hls(url: &str, max_height: u32, audio: Option<&str>) -> String {
    let Some(p) = proxy() else { return url.to_string() };
    let id = register(p, url, None, HLS);
    match audio {
        Some(a) => format!("http://127.0.0.1:{}/h/{id}?max={max_height}&audio={}", p.port, crate::api::http::urlencode(a)),
        None => format!("http://127.0.0.1:{}/h/{id}?max={max_height}", p.port),
    }
}

/// Like `local_hls` for a hover preview: the chosen variant only, without its audio
/// rendition (the preview is muted, and the audio playlist and its first segments
/// would be fetched before the first frame).
pub fn local_hls_preview(url: &str, max_height: u32) -> String {
    let Some(p) = proxy() else { return url.to_string() };
    let id = register(p, url, None, HLS);
    format!("http://127.0.0.1:{}/h/{id}?max={max_height}&noaudio=1", p.port)
}

// ───────────────────────── warm-up for hover previews ─────────────────────────

/// A body fetched ahead of mpv: `None` while the fetch runs, then the result.
type WarmSlot = Arc<(Mutex<Option<Option<Arc<WarmBody>>>>, Condvar)>;

struct WarmBody {
    data: Vec<u8>,
    ctype: String,
}

/// How long fetched-ahead bodies are kept, and how many (a preview takes the master,
/// one variant playlist, the init segment and two media segments).
const WARM_TTL: Duration = Duration::from_secs(600);
const WARM_MAX: usize = 60;

fn warm_map() -> &'static Mutex<HashMap<String, (WarmSlot, Instant)>> {
    static W: OnceLock<Mutex<HashMap<String, (WarmSlot, Instant)>>> = OnceLock::new();
    W.get_or_init(Default::default)
}

/// The slot for `url`, and whether the caller created it (and so has to fill it).
fn warm_slot(url: &str) -> (WarmSlot, bool) {
    let mut m = warm_map().lock();
    m.retain(|_, (_, t)| t.elapsed() < WARM_TTL);
    if let Some((s, _)) = m.get(url) {
        return (s.clone(), false);
    }
    while m.len() >= WARM_MAX {
        let Some(oldest) = m.iter().min_by_key(|(_, (_, t))| *t).map(|(k, _)| k.clone()) else { break };
        m.remove(&oldest);
    }
    let s: WarmSlot = Default::default();
    m.insert(url.to_string(), (s.clone(), Instant::now()));
    (s, true)
}

fn warm_wait(s: &WarmSlot) -> Option<Arc<WarmBody>> {
    let (lock, cv) = &**s;
    let mut g = lock.lock();
    let deadline = Instant::now() + Duration::from_secs(20);
    while g.is_none() {
        if cv.wait_until(&mut g, deadline).timed_out() {
            return None;
        }
    }
    g.clone().flatten()
}

fn warm_set(s: &WarmSlot, body: Option<Arc<WarmBody>>) {
    let (lock, cv) = &**s;
    *lock.lock() = Some(body);
    cv.notify_all();
}

/// A body fetched ahead, waiting for it while its fetch still runs.
fn warm_get(url: &str) -> Option<Arc<WarmBody>> {
    let s = warm_map().lock().get(url).filter(|(_, t)| t.elapsed() < WARM_TTL).map(|(s, _)| s.clone())?;
    warm_wait(&s)
}

/// Fetches `url` once into the warm cache, or joins a fetch already running.
fn warm_fetch(url: &str) -> Option<Arc<WarmBody>> {
    let (s, mine) = warm_slot(url);
    if !mine {
        return warm_wait(&s);
    }
    let mut body = None;
    for attempt in 0..2u64 {
        if let Ok(mut r) = agent().get(url).header("User-Agent", crate::api::http::BROWSER_UA).call() {
            if r.status().as_u16() < 400 {
                let ctype = r.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("application/octet-stream").to_string();
                if let Ok(data) = r.body_mut().with_config().limit(16 << 20).read_to_vec() {
                    body = Some(Arc::new(WarmBody { data, ctype }));
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(300 * (attempt + 1)));
    }
    if body.is_none() {
        crate::perf::preview_mark("warm-up fetch failed");
        // keep no failure: mpv then fetches it itself
        warm_map().lock().remove(url);
    }
    warm_set(&s, body.clone());
    body
}

/// Keeps a master playlist that stream resolving fetched already, so mpv gets it from memory.
pub fn warm_put(url: &str, text: &str) {
    let (s, mine) = warm_slot(url);
    if mine {
        warm_set(&s, Some(Arc::new(WarmBody { data: text.as_bytes().to_vec(), ctype: "application/vnd.apple.mpegurl".into() })));
    }
}

/// Fetches in the background what mpv needs for the first frame of a preview of the
/// HLS master `master_url`: the variant playlist it will pick, the init segment and
/// the first media segments. mpv's own requests are then answered from memory.
pub fn warm_preview(master_url: &str, max_height: u32) {
    let master_url = master_url.to_string();
    std::thread::spawn(move || {
        crate::perf::preview_mark("warm-up start");
        let Some(m) = warm_fetch(&master_url) else { return };
        let text = String::from_utf8_lossy(&m.data).into_owned();
        let Some(uri) = choose_variant(&master_variants(&text), max_height).map(|(_, u)| u.to_string()) else { return };
        let Some(pl) = warm_fetch(&uri) else { return };
        let text = String::from_utf8_lossy(&pl.data).into_owned();
        let fetches: Vec<_> = first_media(&text, 2).into_iter().map(|u| std::thread::spawn(move || warm_fetch(&u))).collect();
        for f in fetches {
            let _ = f.join();
        }
        crate::perf::preview_mark("warm-up done");
    });
}

/// The init segment (EXT-X-MAP) and the first `n` media segments of a media playlist
/// (absolute URLs only; nothing for byte-range playlists, which mpv asks for by range).
fn first_media(text: &str, n: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut segs = 0;
    for l in text.lines().map(str::trim) {
        if l.starts_with("#EXT-X-BYTERANGE") {
            return Vec::new();
        }
        if l.starts_with("#EXT-X-MAP") {
            if let Some(u) = hls_attr(l, "URI").filter(|u| u.starts_with("http")) {
                out.push(u);
            }
        } else if !l.is_empty() && !l.starts_with('#') {
            if l.starts_with("http") {
                out.push(l.to_string());
            }
            segs += 1;
            if segs >= n {
                break;
            }
        }
    }
    out
}

fn hls_attr(line: &str, key: &str) -> Option<String> {
    // match KEY= at an attribute boundary (so AUDIO= doesn't hit "VIDEO-RANGE=..."-like names)
    let mut search = line;
    loop {
        let i = search.find(&format!("{key}="))?;
        let before = search[..i].chars().last();
        let rest = &search[i + key.len() + 1..];
        if matches!(before, Some(':') | Some(',')) {
            return Some(match rest.strip_prefix('"') {
                Some(q) => q.split('"').next()?.to_string(),
                None => rest.split(',').next()?.to_string(),
            });
        }
        search = rest;
    }
}

/// Reduces a master playlist to the one variant mpv should play (best fit for
/// `max_height` and the codec preference) plus its default audio rendition.
/// ffmpeg opens every playlist a master lists before starting, so serving the
/// full master (often 30+ playlists) delays the start by many seconds.
/// The (EXT-X-STREAM-INF line, URI) pairs of a master playlist.
fn master_variants(text: &str) -> Vec<(&str, &str)> {
    let lines: Vec<&str> = text.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
    lines
        .windows(2)
        .filter(|w| w[0].starts_with("#EXT-X-STREAM-INF") && !w[1].starts_with('#'))
        .map(|w| (w[0], w[1]))
        .collect()
}

/// The variant mpv should play: the best fit for `max_height` and the codec preference.
fn choose_variant<'a>(variants: &[(&'a str, &'a str)], max_height: u32) -> Option<(&'a str, &'a str)> {
    let class = |l: &str| -> u32 {
        hls_attr(l, "RESOLUTION")
            .and_then(|r| r.split_once('x').and_then(|(w, h)| Some(crate::api::streams::quality_class(w.parse().ok()?, h.parse().ok()?))))
            .unwrap_or(0)
    };
    let pref = crate::store::settings_snapshot().codec_pref.clone();
    let codec_rank = |l: &str| -> u32 {
        let c = hls_attr(l, "CODECS").unwrap_or_default().to_lowercase();
        let kind = if c.contains("av01") { "av1" } else if c.contains("vp09") || c.contains("vp9") { "vp9" } else { "h264" };
        if kind == pref { 3 } else if kind == "h264" { 2 } else { 1 }
    };
    let bw = |l: &str| hls_attr(l, "BANDWIDTH").and_then(|b| b.parse::<u64>().ok()).unwrap_or(0);
    let fits: Vec<&(&str, &str)> = variants.iter().filter(|(l, _)| max_height == 0 || class(l) <= max_height).collect();
    let chosen = if fits.is_empty() {
        variants.iter().min_by_key(|(l, _)| (class(l), bw(l)))
    } else {
        fits.into_iter().max_by_key(|(l, _)| (class(l), !crate::api::streams::is_hdr_codecs(&hls_attr(l, "CODECS").unwrap_or_default()), codec_rank(l), bw(l)))
    };
    chosen.copied()
}

/// A tag line without one of its attributes (`KEY=value` or `KEY="value"`).
fn without_attr(line: &str, key: &str) -> String {
    let Some((tag, attrs)) = line.split_once(':') else { return line.to_string() };
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for ch in attrs.chars() {
        match ch {
            '"' => {
                quoted = !quoted;
                cur.push(ch);
            }
            ',' if !quoted => parts.push(std::mem::take(&mut cur)),
            _ => cur.push(ch),
        }
    }
    parts.push(cur);
    let prefix = format!("{key}=");
    let kept: Vec<String> = parts.into_iter().filter(|a| !a.starts_with(&prefix)).collect();
    format!("{tag}:{}", kept.join(","))
}

fn rewrite_master(p: &Proxy, text: &str, max_height: u32, audio_name: Option<&str>, video_only: bool) -> String {
    let lines: Vec<&str> = text.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
    let mut header = Vec::new();
    let mut medias = Vec::new();
    let mut variants: Vec<(&str, &str)> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let l = lines[i];
        if l.starts_with("#EXT-X-STREAM-INF") {
            if let Some(u) = lines.get(i + 1).filter(|u| !u.starts_with('#')) {
                variants.push((l, u));
                i += 2;
                continue;
            }
        } else if l.starts_with("#EXT-X-MEDIA") {
            medias.push(l);
        } else if l.starts_with("#EXT-X-I-FRAME-STREAM-INF") || !l.starts_with('#') {
            // i-frame playlists aren't needed for playback
        } else {
            header.push(l);
        }
        i += 1;
    }
    let Some((inf, uri)) = choose_variant(&variants, max_height) else { return rewrite_playlist(p, text, max_height) };
    let inf_line = if video_only { without_attr(inf, "AUDIO") } else { inf.to_string() };
    let local_pl = |u: &str| format!("http://127.0.0.1:{}/h/{}?max=0", p.port, register(p, u, None, HLS));
    let mut out = String::new();
    for h in header {
        out.push_str(h);
        out.push('\n');
    }
    if let Some(group) = hls_attr(inf, "AUDIO").filter(|_| !video_only) {
        let in_group: Vec<&&str> = medias
            .iter()
            .filter(|m| hls_attr(m, "TYPE").as_deref() == Some("AUDIO") && hls_attr(m, "GROUP-ID").as_deref() == Some(group.as_str()))
            .collect();
        // the requested track, else the original, else the default, else the first
        let name_of = |m: &str| crate::api::streams::hls_media_name(m).unwrap_or_default();
        let audio = audio_name
            .and_then(|want| in_group.iter().find(|m| name_of(m) == want))
            .or_else(|| in_group.iter().find(|m| name_of(m).to_lowercase().contains("original")))
            .or_else(|| in_group.iter().find(|m| hls_attr(m, "DEFAULT").as_deref() == Some("YES")))
            .or_else(|| in_group.first())
            .copied();
        if let Some(m) = audio {
            match (m.find("URI=\""), hls_attr(m, "URI")) {
                (Some(i), Some(u)) => {
                    out.push_str(&m[..i + 5]);
                    out.push_str(&local_pl(&u));
                    out.push_str(&m[i + 5 + u.len()..]);
                }
                _ => out.push_str(m),
            }
            out.push('\n');
        }
    }
    out.push_str(&inf_line);
    out.push('\n');
    out.push_str(&local_pl(uri));
    out.push('\n');
    out
}

/// Rewrites one playlist so all its URLs point back at the proxy. Whether a
/// URI is another playlist or media is decided by the tag it belongs to.
fn rewrite_playlist(p: &Proxy, text: &str, max_height: u32) -> String {
    let local = |u: &str, playlist: bool| -> String {
        if playlist {
            format!("http://127.0.0.1:{}/h/{}?max=0", p.port, register(p, u, None, HLS))
        } else {
            format!("http://127.0.0.1:{}/p/{}", p.port, register(p, u, None, PASS))
        }
    };
    let lines: Vec<&str> = text.lines().collect();
    // which variants fit the quality limit (if none do, keep them all)
    let height_of = |l: &str| -> Option<u32> {
        let r = l.split("RESOLUTION=").nth(1)?.split(',').next()?;
        let (w, h) = r.split_once('x')?;
        Some(crate::api::streams::quality_class(w.parse().ok()?, h.parse().ok()?))
    };
    let any_fits = max_height == 0
        || lines.iter().any(|l| l.starts_with("#EXT-X-STREAM-INF") && height_of(l).map(|h| h <= max_height).unwrap_or(true));
    let mut out = String::with_capacity(text.len());
    let mut skip_next_uri = false;
    let mut next_is_playlist = false;
    for line in lines {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        if t.starts_with("#EXT-X-STREAM-INF") {
            if any_fits && max_height > 0 && height_of(t).map(|h| h > max_height).unwrap_or(false) {
                skip_next_uri = true;
                continue;
            }
            next_is_playlist = true;
        }
        if !t.starts_with('#') {
            if skip_next_uri {
                skip_next_uri = false;
                continue;
            }
            out.push_str(&local(t, next_is_playlist));
            next_is_playlist = false;
            out.push('\n');
            continue;
        }
        // tags carrying URIs: renditions / i-frame lists are playlists, EXT-X-MAP and keys are media
        if let Some(i) = t.find("URI=\"") {
            let rest = &t[i + 5..];
            if let Some(j) = rest.find('"') {
                let u = &rest[..j];
                let playlist = t.starts_with("#EXT-X-MEDIA") || t.starts_with("#EXT-X-I-FRAME-STREAM-INF");
                out.push_str(&t[..i + 5]);
                out.push_str(&local(u, playlist));
                out.push_str(&rest[j..]);
                out.push('\n');
                continue;
            }
        }
        out.push_str(t);
        out.push('\n');
    }
    out
}

fn serve_playlist(conn: &mut TcpStream, p: &Proxy, url: &str, max_height: u32, audio: Option<&str>, video_only: bool) -> std::io::Result<()> {
    // from memory only what never changes: a master playlist or a finished (VOD) one.
    // A live media playlist grows with every reload, a stored copy would end it.
    let mut text = warm_get(url)
        .map(|b| String::from_utf8_lossy(&b.data).into_owned())
        .filter(|t| t.contains("#EXT-X-STREAM-INF") || t.contains("#EXT-X-ENDLIST"));
    for attempt in 0..3u64 {
        if text.is_some() {
            break;
        }
        if let Ok(mut r) = agent().get(url).header("User-Agent", crate::api::http::BROWSER_UA).call() {
            if r.status().as_u16() < 400 {
                text = r.body_mut().with_config().limit(16 << 20).read_to_string().ok();
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(300 * (attempt + 1)));
    }
    let Some(text) = text else {
        conn.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
        return Ok(());
    };
    let body = if text.contains("#EXT-X-STREAM-INF") { rewrite_master(p, &text, max_height, audio, video_only) } else { rewrite_playlist(p, &text, max_height) };
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    conn.write_all(head.as_bytes())?;
    conn.write_all(body.as_bytes())?;
    crate::perf::preview_mark(&format!("proxy playlist served ({} bytes upstream)", text.len()));
    conn.flush()
}

/// Plain pass-through (HLS segments): forwards the request and streams the reply.
fn serve_pass(conn: &mut TcpStream, url: &str, range: Option<(u64, Option<u64>)>, head_only: bool) -> std::io::Result<()> {
    // fetched ahead for a preview: from memory (mpv asks for whole segments, or from 0)
    if !head_only && matches!(range, None | Some((0, None))) {
        if let Some(b) = warm_get(url) {
            let head = format!("HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", b.ctype, b.data.len());
            conn.write_all(head.as_bytes())?;
            conn.write_all(&b.data)?;
            crate::perf::preview_mark(&format!("proxy segment from memory ({} bytes)", b.data.len()));
            return conn.flush();
        }
    }
    let mut resp = None;
    for attempt in 0..3u64 {
        let mut req = agent().get(url).header("User-Agent", crate::api::http::BROWSER_UA);
        let rg;
        if let Some((a, b)) = range {
            rg = match b {
                Some(b) => format!("bytes={a}-{b}"),
                None => format!("bytes={a}-"),
            };
            req = req.header("Range", &rg);
        }
        match req.call() {
            Ok(r) if r.status().as_u16() < 400 => {
                resp = Some(r);
                break;
            }
            _ => std::thread::sleep(std::time::Duration::from_millis(300 * (attempt + 1))),
        }
    }
    let Some(r) = resp else {
        crate::perf::preview_mark("proxy segment failed upstream");
        conn.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
        return Ok(());
    };
    let status = r.status().as_u16();
    let hdr = |k: &str| r.headers().get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
    let mut head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: {}\r\nConnection: close\r\n",
        if status == 206 { "Partial Content" } else { "OK" },
        hdr("content-type").unwrap_or_else(|| "application/octet-stream".into())
    );
    if let Some(l) = hdr("content-length") {
        head.push_str(&format!("Content-Length: {l}\r\n"));
    }
    if let Some(cr) = hdr("content-range") {
        head.push_str(&format!("Content-Range: {cr}\r\n"));
    }
    head.push_str("\r\n");
    conn.write_all(head.as_bytes())?;
    if head_only {
        return Ok(());
    }
    crate::perf::preview_mark("proxy segment headers");
    let mut body = r.into_body().into_reader();
    let n = std::io::copy(&mut body, conn)?;
    crate::perf::preview_mark(&format!("proxy segment done ({n} bytes)"));
    conn.flush()
}

fn agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        let mut b = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .ip_family(crate::store::ip_family())
            .timeout_connect(Some(std::time::Duration::from_secs(10)))
            .timeout_recv_body(Some(std::time::Duration::from_secs(30)));
        // video data goes through the user's proxy too (HTTP or SOCKS)
        if let Some(p) = crate::store::proxy_setting() {
            if let Ok(proxy) = ureq::Proxy::new(&p) {
                b = b.proxy(Some(proxy));
            }
        }
        b.build().new_agent()
    })
}

/// Learns the total size with a 1-byte request when the API didn't provide it.
fn probe_len(url: &str) -> Option<u64> {
    let r = agent().get(url).header("User-Agent", crate::api::http::BROWSER_UA).header("Range", "bytes=0-0").call().ok()?;
    let cr = r.headers().get("content-range")?.to_str().ok()?.to_string();
    cr.rsplit('/').next()?.trim().parse().ok()
}

fn serve(mut conn: TcpStream) -> std::io::Result<()> {
    conn.set_nodelay(true).ok();
    let mut reader = BufReader::new(conn.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut range: Option<(u64, Option<u64>)> = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("range:") {
            if let Some(spec) = v.trim().strip_prefix("bytes=") {
                let (a, b) = spec.split_once('-').unwrap_or((spec, ""));
                if let Ok(a) = a.trim().parse::<u64>() {
                    range = Some((a, b.trim().parse::<u64>().ok()));
                }
            }
        }
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let Some(p) = proxy() else { return Ok(()) };
    if crate::perf::on() {
        crate::perf::preview_mark(&format!("proxy {method} {} range={range:?}", path.split('?').next().unwrap_or(path)));
    }
    let lookup =|rest: &str| -> Option<String> {
        let id: u64 = rest.split('?').next()?.parse().ok()?;
        p.streams.lock().get(&id).map(|e| e.url.clone())
    };
    if let Some(rest) = path.strip_prefix("/h/") {
        let query = rest.split_once('?').map(|q| q.1).unwrap_or("");
        let param = |k: &str| query.split('&').find_map(|kv| kv.strip_prefix(k).and_then(|v| v.strip_prefix('=')));
        let max = param("max").and_then(|m| m.parse().ok()).unwrap_or(0);
        let audio = param("audio").map(crate::api::http::urldecode);
        let video_only = param("noaudio") == Some("1");
        return match lookup(rest) {
            Some(url) => serve_playlist(&mut conn, p, &url, max, audio.as_deref(), video_only),
            None => conn.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
        };
    }
    if let Some(rest) = path.strip_prefix("/p/") {
        return match lookup(rest) {
            Some(url) => serve_pass(&mut conn, &url, range, method == "HEAD"),
            None => conn.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
        };
    }
    let id: Option<u64> = path.strip_prefix("/s/").and_then(|s| s.parse().ok());
    let entry = id.and_then(|id| p.streams.lock().get(&id).map(|e| (e.url.clone(), e.len, e.mime.clone(), e.preview.clone())));
    let Some((url, mut len, mime, preview)) = entry else {
        conn.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
        return Ok(());
    };
    let stale = |preview: &Option<(String, u64)>| preview.as_ref().is_some_and(|(_, g)| *g != PREVIEW_GEN.load(Ordering::SeqCst));
    if stale(&preview) {
        conn.write_all(b"HTTP/1.1 410 Gone\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
        return Ok(());
    }
    if len.is_none() {
        len = probe_len(&url);
        if let (Some(id), Some(l)) = (id, len) {
            if let Some(e) = p.streams.lock().get_mut(&id) {
                e.len = Some(l);
            }
        }
    }
    let Some(total) = len else {
        conn.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
        return Ok(());
    };
    let (start, end) = match range {
        Some((a, b)) => (a.min(total), b.unwrap_or(total - 1).min(total - 1)),
        None => (0, total.saturating_sub(1)),
    };
    if start >= total {
        let h = format!("HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        conn.write_all(h.as_bytes())?;
        return Ok(());
    }
    let body_len = end - start + 1;
    let head = if range.is_some() {
        format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Type: {mime}\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{total}\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n"
        )
    } else {
        format!("HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nAccept-Ranges: bytes\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n")
    };
    conn.write_all(head.as_bytes())?;
    if method == "HEAD" {
        return Ok(());
    }
    let mut pos = start;
    // a preview hovered again: what is in memory goes first, the network only for the rest
    if let Some((key, _)) = &preview {
        let cached = preview_cache().lock().slice(key, start, end);
        if !cached.is_empty() {
            conn.write_all(&cached)?;
            pos += cached.len() as u64;
        }
    }
    let mut first = true;
    let mut stalls = 0;
    let mut buf = vec![0u8; 64 * 1024];
    while pos <= end {
        if stale(&preview) {
            return Ok(());
        }
        let size = if first { FIRST_CHUNK } else { CHUNK };
        first = false;
        let chunk_end = (pos + size - 1).min(end);
        let mut attempt = 0;
        loop {
            let resp = agent()
                .get(&url)
                .header("User-Agent", crate::api::http::BROWSER_UA)
                .header("Range", &format!("bytes={pos}-{chunk_end}"))
                .call();
            if crate::perf::on() {
                match &resp {
                    Ok(r) => eprintln!("[proxy] {pos}-{chunk_end} -> {}", r.status()),
                    Err(e) => eprintln!("[proxy] {pos}-{chunk_end} -> error {e}"),
                }
            }
            match resp {
                Ok(r) if r.status().as_u16() == 206 || r.status().as_u16() == 200 => {
                    let mut body = r.into_body().into_reader();
                    loop {
                        let n = match body.read(&mut buf) {
                            Ok(n) => n,
                            Err(e) => {
                                if crate::perf::on() {
                                    eprintln!("[proxy] upstream read error at {pos}: {e}");
                                }
                                break;
                            }
                        };
                        if n == 0 {
                            break;
                        }
                        // a write error means mpv closed the connection (e.g. it seeked)
                        if let Err(e) = conn.write_all(&buf[..n]) {
                            if crate::perf::on() {
                                eprintln!("[proxy] client closed at {pos}: {e}");
                            }
                            return Err(e);
                        }
                        if let Some((key, _)) = &preview {
                            preview_cache().lock().append(key, pos, &buf[..n]);
                            if stale(&preview) {
                                // the preview ended: drop the upstream connection now
                                return Ok(());
                            }
                        }
                        pos += n as u64;
                    }
                    break;
                }
                _ if attempt < 3 => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(300 * attempt));
                }
                _ => return Ok(()),
            }
        }
        if pos <= chunk_end {
            // upstream ended early; retry from where we are (but don't spin forever)
            stalls += 1;
            if stalls > 5 {
                return Ok(());
            }
        }
    }
    conn.flush()
}

#[cfg(test)]
mod preview_cache_tests {
    use super::*;

    #[test]
    fn keeps_beginnings_within_limits() {
        let mut c = PreviewCache::default();
        c.append("a", 0, &[1, 2, 3]);
        c.append("a", 5, &[9]); // not a continuation: ignored
        c.append("a", 3, &[4, 5]);
        assert_eq!(c.slice("a", 1, 3), vec![2, 3, 4]);
        assert_eq!(c.slice("a", 2, 100), vec![3, 4, 5]);
        assert!(c.slice("a", 5, 10).is_empty());
        assert!(c.slice("b", 0, 10).is_empty());
        // one stream never takes more than its share
        let big = vec![0u8; PREVIEW_ENTRY_MAX + 10];
        c.append("big", 0, &big);
        assert_eq!(c.slice("big", 0, u64::MAX - 1).len(), PREVIEW_ENTRY_MAX);
        // and all of them stay under the total: the least recently used go first
        for i in 0..(PREVIEW_CACHE_MAX / PREVIEW_ENTRY_MAX + 3) {
            c.append(&format!("v{i}"), 0, &big);
        }
        assert!(c.total <= PREVIEW_CACHE_MAX);
        assert_eq!(c.total, c.map.values().map(|(d, _)| d.len()).sum::<usize>());
        assert!(!c.map.contains_key("a"));
    }

    #[test]
    fn preview_playlists() {
        let master = "#EXTM3U\n#EXT-X-INDEPENDENT-SEGMENTS\n\
            #EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"234\",NAME=\"Default\",DEFAULT=YES,URI=\"https://a/audio.m3u8\"\n\
            #EXT-X-STREAM-INF:BANDWIDTH=300000,CODECS=\"avc1.4d401e,mp4a.40.2\",RESOLUTION=640x360,AUDIO=\"234\"\n\
            https://a/360.m3u8\n\
            #EXT-X-STREAM-INF:BANDWIDTH=900000,CODECS=\"avc1.4d401f,mp4a.40.2\",RESOLUTION=1280x720,AUDIO=\"234\"\n\
            https://a/720.m3u8\n";
        let v = master_variants(master);
        assert_eq!(v.len(), 2);
        assert_eq!(choose_variant(&v, 360).map(|(_, u)| u), Some("https://a/360.m3u8"));
        assert_eq!(
            without_attr(v[0].0, "AUDIO"),
            "#EXT-X-STREAM-INF:BANDWIDTH=300000,CODECS=\"avc1.4d401e,mp4a.40.2\",RESOLUTION=640x360"
        );
        let media = "#EXTM3U\n#EXT-X-TARGETDURATION:5\n#EXT-X-MAP:URI=\"https://a/init\"\n#EXTINF:5,\nhttps://a/s1\n#EXTINF:5,\nhttps://a/s2\n#EXTINF:5,\nhttps://a/s3\n";
        assert_eq!(first_media(media, 2), vec!["https://a/init", "https://a/s1", "https://a/s2"]);
        assert!(first_media("#EXTM3U\n#EXT-X-BYTERANGE:100@0\nhttps://a/f\n", 2).is_empty());
    }

    #[test]
    fn live_reloads_keep_ids() {
        let p = Proxy { port: 0, streams: Mutex::new(HashMap::new()), ids: Mutex::new(HashMap::new()), next: Mutex::new(0) };
        let pl = register(&p, "https://a/live.m3u8", None, HLS);
        let s1 = register(&p, "https://a/sq/1", None, PASS);
        // a reload lists the same segment again: same id, no new entry
        assert_eq!(register(&p, "https://a/sq/1", None, PASS), s1);
        // many segments later the playlist is still there
        for i in 0..25_000 {
            register(&p, &format!("https://a/sq/x{i}"), None, PASS);
        }
        assert!(p.streams.lock().contains_key(&pl));
        assert!(p.streams.lock().len() <= 20_001);
        assert_eq!(register(&p, "https://a/live.m3u8", None, HLS), pl);
    }

    #[test]
    fn warm_bodies_are_shared() {
        warm_put("https://a/master-test", "#EXTM3U");
        assert_eq!(warm_get("https://a/master-test").map(|b| b.data.clone()), Some(b"#EXTM3U".to_vec()));
        assert!(warm_get("https://a/never").is_none());
    }

    #[test]
    fn generations_move_on() {
        let a = preview_next();
        let b = preview_next();
        assert!(b > a);
    }
}
