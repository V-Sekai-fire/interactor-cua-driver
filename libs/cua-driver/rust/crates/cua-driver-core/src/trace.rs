//! Every tool call as a span, kept in SQLite in the schema XR Pilot's `TraceStore` writes
//! (V-Sekai-fire/interactor-xr-pilot `core/src/TraceStore.cpp`, schema version 3), so the same readers
//! work on both. The store is in Essential Tuple Normal Form: facts that may be missing (an end, a
//! parent, a detail, a score, an image's extent) are satellite relations, never nullable columns.
//!
//! A span that acts on a window is also a MaskScore row: its input is the target window before the
//! call (`source_view`), its candidate the window after it (axis `view`, kind `after_span`), and its
//! instruction the tool and its arguments, so the `editscore_pair` view yields EditScore triples.
//! XR Pilot's view is a streamed PyroWave frame; here it is the window as a PNG (asset kind `png`).
//!
//! Tracing never fails or delays the action it observes: events go to a bounded queue with
//! `try_send`, and a writer thread owns the database. A full queue drops the event and counts it.
//!
//! On by default. `CUA_DRIVER_TRACE=off` turns it off; `CUA_DRIVER_TRACE=<path>` picks the file.
//! The default file is `traces.sqlite` in the per-user data directory under `V-Sekai-fire/cua-tracer`
//! (XR Pilot's is under `V-Sekai-fire/xr-pilot`).

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Mutex, OnceLock,
    },
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

/// Bumped with XR Pilot's: an older store is moved aside, not altered.
pub const SCHEMA_VERSION: i64 = 3;
const QUEUE_CAPACITY: usize = 256;

/// The interned vocabularies, one relation each (XR Pilot's list).
const VOCABULARIES: &[&str] = &[
    "tool",
    "task_type",
    "dimension",
    "input_column",
    "asset_kind",
    "candidate_axis",
    "candidate_kind",
    "metric",
];

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS session (
    session_id INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS span (
    span_id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES session,
    span_key TEXT NOT NULL,
    tool_id INTEGER NOT NULL REFERENCES tool,
    started TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS span_detail (
    span_id INTEGER PRIMARY KEY REFERENCES span,
    detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS span_parent (
    span_id INTEGER PRIMARY KEY REFERENCES span,
    parent_span_id INTEGER NOT NULL REFERENCES span);
CREATE TABLE IF NOT EXISTS span_end (
    span_id INTEGER PRIMARY KEY REFERENCES span,
    ended TEXT NOT NULL,
    ok INTEGER NOT NULL CHECK (ok IN (0, 1)));
CREATE TABLE IF NOT EXISTS span_command (
    span_id INTEGER NOT NULL REFERENCES span,
    seq INTEGER NOT NULL,
    at TEXT NOT NULL,
    command TEXT NOT NULL,
    ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
    PRIMARY KEY (span_id, seq));
CREATE TABLE IF NOT EXISTS asset (
    asset_id INTEGER PRIMARY KEY,
    asset_kind_id INTEGER NOT NULL REFERENCES asset_kind,
    bytes BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS asset_extent (
    asset_id INTEGER PRIMARY KEY REFERENCES asset,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS maskscore_root (
    span_id INTEGER PRIMARY KEY REFERENCES span,
    task_type_id INTEGER NOT NULL REFERENCES task_type,
    dimension_id INTEGER NOT NULL REFERENCES dimension);
CREATE TABLE IF NOT EXISTS maskscore_input (
    span_id INTEGER NOT NULL REFERENCES maskscore_root,
    input_column_id INTEGER NOT NULL REFERENCES input_column,
    input_asset_id INTEGER NOT NULL REFERENCES asset,
    PRIMARY KEY (span_id, input_column_id));
CREATE TABLE IF NOT EXISTS maskscore_candidate (
    span_id INTEGER NOT NULL REFERENCES maskscore_root,
    candidate_axis_id INTEGER NOT NULL REFERENCES candidate_axis,
    rank INTEGER NOT NULL,
    candidate_asset_id INTEGER NOT NULL REFERENCES asset,
    candidate_kind_id INTEGER NOT NULL REFERENCES candidate_kind,
    PRIMARY KEY (span_id, candidate_axis_id, rank));
CREATE TABLE IF NOT EXISTS maskscore_score (
    span_id INTEGER NOT NULL,
    candidate_axis_id INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    metric_id INTEGER NOT NULL REFERENCES metric,
    metric_value REAL NOT NULL,
    PRIMARY KEY (span_id, candidate_axis_id, rank, metric_id),
    FOREIGN KEY (span_id, candidate_axis_id, rank) REFERENCES maskscore_candidate);
CREATE VIEW IF NOT EXISTS editscore_pair AS
SELECT r.span_id, s.session_id, sk.name AS source_kind, src.bytes AS source, ek.name AS edited_kind,
       ed.bytes AS edited, t.name || COALESCE(' ' || d.detail, '') AS instruction
FROM maskscore_root r
JOIN span s USING (span_id)
JOIN tool t USING (tool_id)
LEFT JOIN span_detail d USING (span_id)
JOIN maskscore_input i ON i.span_id = r.span_id
JOIN input_column ic ON ic.input_column_id = i.input_column_id AND ic.name = 'source_view'
JOIN asset src ON src.asset_id = i.input_asset_id
JOIN asset_kind sk ON sk.asset_kind_id = src.asset_kind_id
JOIN maskscore_candidate c ON c.span_id = r.span_id AND c.rank = 1
JOIN candidate_axis ca ON ca.candidate_axis_id = c.candidate_axis_id AND ca.name = 'view'
JOIN asset ed ON ed.asset_id = c.candidate_asset_id
JOIN asset_kind ek ON ek.asset_kind_id = ed.asset_kind_id;
"#;

/// Argument keys whose values are what a person typed or pasted; the trace keeps their length only,
/// so a password typed through `type_text` never lands in the store.
const REDACTED_KEYS: &[&str] = &["text", "value", "password", "secret", "token", "clipboard"];

enum Event {
    Begin {
        key: u64,
        parent: Option<u64>,
        tool: String,
        detail: String,
        at_ms: i64,
        view: Option<Vec<u8>>,
    },
    End {
        key: u64,
        ok: bool,
        at_ms: i64,
        view: Option<Vec<u8>>,
    },
    Flush(SyncSender<()>),
}

/// A span begun and not yet ended; hand it back to [`TraceStore::end`].
#[derive(Debug)]
pub struct OpenSpan {
    key: u64,
}

pub struct TraceStore {
    tx: SyncSender<Event>,
    epoch: Instant,
    next_key: AtomicU64,
    running: Mutex<Vec<u64>>,
    dropped: AtomicU64,
    path: PathBuf,
}

static GLOBAL: OnceLock<Option<TraceStore>> = OnceLock::new();

/// The process's store, opened on first use from `CUA_DRIVER_TRACE`; `None` when tracing is off or
/// the file cannot be opened (the reason goes to the log once).
pub fn global() -> Option<&'static TraceStore> {
    GLOBAL
        .get_or_init(|| {
            let path = match std::env::var("CUA_DRIVER_TRACE") {
                Ok(v) if v.eq_ignore_ascii_case("off") || v == "0" => return None,
                Ok(v) if !v.is_empty() => PathBuf::from(v),
                _ => dirs::data_dir()?.join("V-Sekai-fire").join("cua-tracer").join("traces.sqlite"),
            };
            match TraceStore::open(&path) {
                Ok(store) => Some(store),
                Err(error) => {
                    tracing::warn!("trace store off: cannot open {}: {error:#}", path.display());
                    None
                }
            }
        })
        .as_ref()
}

impl TraceStore {
    /// Creates the schema if needed and starts a session.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn = open_schema(path)?;
        let started_at = civil_time(unix_ms());
        conn.execute("INSERT INTO session (started_at) VALUES (?1)", params![started_at])?;
        let session = conn.last_insert_rowid();
        let (tx, rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        let epoch = Instant::now();
        thread::Builder::new()
            .name("cua-trace-writer".into())
            .spawn(move || Writer::new(conn, session).run(rx))?;
        Ok(Self {
            tx,
            epoch,
            next_key: AtomicU64::new(1),
            running: Mutex::new(Vec::new()),
            dropped: AtomicU64::new(0),
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Events dropped because the writer fell behind.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn now_ms(&self) -> i64 {
        self.epoch.elapsed().as_millis() as i64
    }

    fn send(&self, event: Event) {
        match self.tx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Opens a span for a tool call. A span begun while another runs nests inside it, as in XR Pilot.
    /// `view` is the target window before the call, as PNG, when the call acts on a window.
    pub fn begin(&self, tool: &str, args: &Value, view: Option<Vec<u8>>) -> OpenSpan {
        let key = self.next_key.fetch_add(1, Ordering::Relaxed);
        let parent = {
            let mut running = self.running.lock().unwrap_or_else(|e| e.into_inner());
            let parent = running.last().copied();
            running.push(key);
            parent
        };
        self.send(Event::Begin {
            key,
            parent,
            tool: tool.to_string(),
            detail: detail(args),
            at_ms: self.now_ms(),
            view,
        });
        OpenSpan { key }
    }

    /// Ends a span; `view` is the target window after the call.
    pub fn end(&self, span: OpenSpan, ok: bool, view: Option<Vec<u8>>) {
        {
            let mut running = self.running.lock().unwrap_or_else(|e| e.into_inner());
            running.retain(|k| *k != span.key);
        }
        self.send(Event::End {
            key: span.key,
            ok,
            at_ms: self.now_ms(),
            view,
        });
    }

    /// Waits until every event sent so far is written.
    pub fn flush(&self) {
        let (tx, rx) = mpsc::sync_channel(1);
        if self.tx.send(Event::Flush(tx)).is_ok() {
            let _ = rx.recv();
        }
    }
}

fn open_schema(path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(std::time::Duration::from_millis(2000))?;
    let have: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    let tables: i64 =
        conn.query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table'", [], |r| r.get(0))?;
    if tables > 0 && have != SCHEMA_VERSION {
        // An older store keeps its rows under its own name; this one starts over.
        drop(conn);
        let aside = PathBuf::from(format!("{}.v{have}", path.display()));
        std::fs::rename(path, &aside)?;
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
        return open_schema(path);
    }
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
    for table in VOCABULARIES {
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {table} ({table}_id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);"
        ))?;
    }
    conn.execute_batch(SCHEMA)?;
    conn.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
    Ok(conn)
}

/// The arguments as compact JSON, typed text replaced by its length and the driver's own
/// underscore-prefixed plumbing (transport ids, session ids) left out.
pub fn detail(args: &Value) -> String {
    fn scrub(v: &Value) -> Value {
        match v {
            Value::Object(map) => Value::Object(
                map.iter()
                    .filter(|(k, _)| !k.starts_with('_'))
                    .map(|(k, v)| {
                        let v = match (REDACTED_KEYS.contains(&k.as_str()), v) {
                            (true, Value::String(s)) => Value::String(format!("<{} chars>", s.chars().count())),
                            (true, Value::Null) => Value::Null,
                            (true, _) => Value::String("<redacted>".into()),
                            (false, v) => scrub(v),
                        };
                        (k.clone(), v)
                    })
                    .collect(),
            ),
            Value::Array(items) => Value::Array(items.iter().map(scrub).collect()),
            other => other.clone(),
        }
    }
    match scrub(args) {
        Value::Object(map) if map.is_empty() => String::new(),
        Value::Null => String::new(),
        v => v.to_string(),
    }
}

struct Writer {
    conn: Connection,
    session: i64,
    spans: HashMap<u64, i64>,
    input_thumb: HashMap<i64, (u32, u32, Vec<u8>)>,
    vocab: HashMap<(&'static str, String), i64>,
}

impl Writer {
    fn new(conn: Connection, session: i64) -> Self {
        Self {
            conn,
            session,
            spans: HashMap::new(),
            input_thumb: HashMap::new(),
            vocab: HashMap::new(),
        }
    }

    fn run(mut self, rx: Receiver<Event>) {
        for event in rx {
            let result = match event {
                Event::Flush(done) => {
                    let _ = done.send(());
                    Ok(())
                }
                event => self.write(event),
            };
            if let Err(error) = result {
                tracing::warn!("trace store write failed: {error:#}");
            }
        }
    }

    fn intern(&mut self, table: &'static str, name: &str) -> rusqlite::Result<i64> {
        if let Some(id) = self.vocab.get(&(table, name.to_string())) {
            return Ok(*id);
        }
        self.conn
            .execute(&format!("INSERT OR IGNORE INTO {table} (name) VALUES (?1)"), params![name])?;
        let id: i64 = self.conn.query_row(
            &format!("SELECT {table}_id FROM {table} WHERE name = ?1"),
            params![name],
            |r| r.get(0),
        )?;
        self.vocab.insert((table, name.to_string()), id);
        Ok(id)
    }

    fn asset(&mut self, png: &[u8]) -> rusqlite::Result<(i64, Option<(u32, u32, Vec<u8>)>)> {
        let kind = self.intern("asset_kind", "png")?;
        self.conn
            .execute("INSERT INTO asset (asset_kind_id, bytes) VALUES (?1, ?2)", params![kind, png])?;
        let id = self.conn.last_insert_rowid();
        let thumb = thumb(png);
        if let Some((w, h, _)) = &thumb {
            self.conn
                .execute("INSERT INTO asset_extent VALUES (?1, ?2, ?3)", params![id, w, h])?;
        }
        Ok((id, thumb))
    }

    fn write(&mut self, event: Event) -> rusqlite::Result<()> {
        self.conn.execute_batch("BEGIN")?;
        let result = self.write_in_transaction(event);
        self.conn
            .execute_batch(if result.is_ok() { "COMMIT" } else { "ROLLBACK" })?;
        result
    }

    fn write_in_transaction(&mut self, event: Event) -> rusqlite::Result<()> {
        match event {
            Event::Begin { key, parent, tool, detail, at_ms, view } => {
                let tool_id = self.intern("tool", &tool)?;
                self.conn.execute(
                    "INSERT INTO span (session_id, span_key, tool_id, started) VALUES (?1, ?2, ?3, ?4)",
                    params![self.session, key.to_string(), tool_id, iso_duration(at_ms)],
                )?;
                let id = self.conn.last_insert_rowid();
                self.spans.insert(key, id);
                if !detail.is_empty() {
                    self.conn
                        .execute("INSERT INTO span_detail VALUES (?1, ?2)", params![id, detail])?;
                }
                if let Some(parent) = parent.and_then(|p| self.spans.get(&p).copied()) {
                    self.conn
                        .execute("INSERT INTO span_parent VALUES (?1, ?2)", params![id, parent])?;
                }
                if let Some(png) = view {
                    let (asset, thumb) = self.asset(&png)?;
                    let task = self.intern("task_type", "agent_step")?;
                    let dimension = self.intern("dimension", "instruction_following")?;
                    let column = self.intern("input_column", "source_view")?;
                    self.conn.execute(
                        "INSERT OR IGNORE INTO maskscore_root VALUES (?1, ?2, ?3)",
                        params![id, task, dimension],
                    )?;
                    self.conn.execute(
                        "INSERT OR IGNORE INTO maskscore_input VALUES (?1, ?2, ?3)",
                        params![id, column, asset],
                    )?;
                    if let Some(t) = thumb {
                        self.input_thumb.insert(id, t);
                    }
                }
            }
            Event::End { key, ok, at_ms, view } => {
                let Some(id) = self.spans.remove(&key) else {
                    return Ok(());
                };
                self.conn.execute(
                    "INSERT OR REPLACE INTO span_end VALUES (?1, ?2, ?3)",
                    params![id, iso_duration(at_ms), ok as i64],
                )?;
                let rooted: Option<i64> = self
                    .conn
                    .query_row("SELECT span_id FROM maskscore_root WHERE span_id = ?1", params![id], |r| {
                        r.get(0)
                    })
                    .optional()?;
                let before = self.input_thumb.remove(&id);
                if let (Some(png), Some(_)) = (view, rooted) {
                    let (asset, after) = self.asset(&png)?;
                    let axis = self.intern("candidate_axis", "view")?;
                    let kind = self.intern("candidate_kind", "after_span")?;
                    self.conn.execute(
                        "INSERT OR IGNORE INTO maskscore_candidate VALUES (?1, ?2, 1, ?3, ?4)",
                        params![id, axis, asset, kind],
                    )?;
                    if let (Some((bw, bh, b)), Some((aw, ah, a))) = (before, after) {
                        if (bw, bh) == (aw, ah) && !a.is_empty() {
                            let metric = self.intern("metric", "gray_l1_8x8")?;
                            self.conn.execute(
                                "INSERT OR REPLACE INTO maskscore_score VALUES (?1, ?2, 1, ?3, ?4)",
                                params![id, axis, metric, gray_l1(&b, &a)],
                            )?;
                        }
                    }
                }
            }
            Event::Flush(_) => {}
        }
        Ok(())
    }
}

/// One gray value per 8x8 block, as XR Pilot's metric reads a frame: (g = (r + 2g + b) / 4) at each
/// block's top-left pixel. Returns the image's extent with it; `None` when the PNG does not decode.
fn thumb(png: &[u8]) -> Option<(u32, u32, Vec<u8>)> {
    let img = image::load_from_memory_with_format(png, image::ImageFormat::Png).ok()?.to_rgba8();
    let (w, h) = img.dimensions();
    let mut out = Vec::new();
    let mut y = 0;
    while y + 8 <= h {
        let mut x = 0;
        while x + 8 <= w {
            let p = img.get_pixel(x, y).0;
            out.push(((p[0] as u32 + 2 * p[1] as u32 + p[2] as u32) / 4) as u8);
            x += 8;
        }
        y += 8;
    }
    Some((w, h, out))
}

/// Mean absolute difference of two thumbs, 0..1.
fn gray_l1(a: &[u8], b: &[u8]) -> f64 {
    let sum: u64 = a.iter().zip(b).map(|(x, y)| (*x as i32 - *y as i32).unsigned_abs() as u64).sum();
    sum as f64 / a.len() as f64 / 255.0
}

fn unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// RECTGTN's offset form, as XR Pilot writes it: `PT1M2.5S`.
pub fn iso_duration(ms: i64) -> String {
    let ms = ms.max(0);
    let (hours, minutes, seconds, millis) = (ms / 3_600_000, ms / 60_000 % 60, ms / 1000 % 60, ms % 1000);
    let mut out = String::from("PT");
    if hours > 0 {
        out += &format!("{hours}H");
    }
    if minutes > 0 {
        out += &format!("{minutes}M");
    }
    if seconds > 0 || millis > 0 || out == "PT" {
        out += &seconds.to_string();
        if millis > 0 {
            out += format!(".{millis:03}").trim_end_matches('0');
        }
        out += "S";
    }
    out
}

/// Civil time, ISO 8601 UTC with milliseconds: `2026-10-04T17:02:13.123Z`.
pub fn civil_time(unix_ms: i64) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp_nanos(unix_ms as i128 * 1_000_000)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.millisecond()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn png(w: u32, h: u32, shade: u8) -> Vec<u8> {
        let img = image::RgbaImage::from_pixel(w, h, image::Rgba([shade, shade, shade, 255]));
        let mut out = std::io::Cursor::new(Vec::new());
        img.write_to(&mut out, image::ImageFormat::Png).unwrap();
        out.into_inner()
    }

    fn temp_db(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cua-trace-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("traces.sqlite")
    }

    #[test]
    fn durations_and_civil_time_match_xr_pilot() {
        assert_eq!(iso_duration(0), "PT0S");
        assert_eq!(iso_duration(2500), "PT2.5S");
        assert_eq!(iso_duration(62_500), "PT1M2.5S");
        assert_eq!(iso_duration(3_600_000), "PT1H");
        assert_eq!(iso_duration(-5), "PT0S");
        assert_eq!(civil_time(1_791_313_333_123), "2026-10-06T19:02:13.123Z"); // checked against Python utcfromtimestamp
        assert_eq!(civil_time(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn typed_text_keeps_only_its_length() {
        let d = detail(&json!({"pid": 7, "text": "hunter2", "_transport_session_id": "x"}));
        assert_eq!(d, r#"{"pid":7,"text":"<7 chars>"}"#);
        assert!(!d.contains("hunter2"));
        // A control: a key that is not redacted keeps its value.
        assert!(detail(&json!({"keys": "ctrl+s"})).contains("ctrl+s"));
        assert_eq!(detail(&json!({})), "");
    }

    #[test]
    fn spans_nest_end_and_form_an_editscore_pair() {
        let path = temp_db("pair");
        let store = TraceStore::open(&path).unwrap();
        let outer = store.begin("run_steps", &json!({}), None);
        let inner = store.begin("click", &json!({"pid": 1, "x": 3}), Some(png(64, 32, 10)));
        store.end(inner, true, Some(png(64, 32, 200)));
        store.end(outer, false, None);
        store.flush();

        let conn = Connection::open(&path).unwrap();
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let spans: i64 = conn.query_row("SELECT count(*) FROM span", [], |r| r.get(0)).unwrap();
        assert_eq!(spans, 2);
        let (child_tool, parent_tool): (String, String) = conn
            .query_row(
                "SELECT tc.name, tp.name FROM span_parent p JOIN span c ON c.span_id = p.span_id \
                 JOIN tool tc ON tc.tool_id = c.tool_id JOIN span s ON s.span_id = p.parent_span_id \
                 JOIN tool tp ON tp.tool_id = s.tool_id",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((child_tool.as_str(), parent_tool.as_str()), ("click", "run_steps"));
        let ends: Vec<i64> = conn
            .prepare("SELECT ok FROM span_end ORDER BY span_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(ends, vec![0, 1]);
        let instruction: String =
            conn.query_row("SELECT instruction FROM editscore_pair", [], |r| r.get(0)).unwrap();
        assert_eq!(instruction, r#"click {"pid":1,"x":3}"#);
        let score: f64 = conn.query_row("SELECT metric_value FROM maskscore_score", [], |r| r.get(0)).unwrap();
        assert!((score - 190.0 / 255.0).abs() < 1e-9, "score {score}");
        // The ETNF rule XR Pilot checks: no column anywhere holds a NULL.
        for table in ["span", "span_detail", "span_parent", "span_end", "asset", "asset_extent", "maskscore_root",
            "maskscore_input", "maskscore_candidate", "maskscore_score"] {
            let cols: Vec<String> = conn
                .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            for c in cols {
                let nulls: i64 =
                    conn.query_row(&format!("SELECT count(*) FROM {table} WHERE {c} IS NULL"), [], |r| r.get(0)).unwrap();
                assert_eq!(nulls, 0, "{table}.{c}");
            }
        }
    }

    #[test]
    fn a_span_without_a_view_has_no_maskscore_row() {
        let path = temp_db("noview");
        let store = TraceStore::open(&path).unwrap();
        let s = store.begin("list_windows", &json!({}), None);
        store.end(s, true, None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let roots: i64 = conn.query_row("SELECT count(*) FROM maskscore_root", [], |r| r.get(0)).unwrap();
        assert_eq!(roots, 0);
        let details: i64 = conn.query_row("SELECT count(*) FROM span_detail", [], |r| r.get(0)).unwrap();
        assert_eq!(details, 0, "empty arguments are a missing row, not an empty string");
    }

    #[test]
    fn an_older_store_is_moved_aside() {
        let path = temp_db("aside");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        {
            let c = Connection::open(&path).unwrap();
            c.execute_batch("CREATE TABLE old (x); PRAGMA user_version = 2;").unwrap();
        }
        let _store = TraceStore::open(&path).unwrap();
        assert!(PathBuf::from(format!("{}.v2", path.display())).exists());
    }
}
