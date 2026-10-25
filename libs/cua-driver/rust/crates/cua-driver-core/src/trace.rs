//! Every tool call as a span, kept in SQLite in the schema XR Pilot's `TraceStore` writes
//! (V-Sekai-fire/interactor-xr-pilot `core/src/TraceStore.cpp`, schema version 4), so the same readers
//! work on both. The store is in Essential Tuple Normal Form: facts that may be missing (an end, a
//! parent, a detail, a gold answer, a measure, an image's extent) are satellite relations, never
//! nullable columns.
//!
//! A span that acts on a window is also a decider example, the shape of chibifire/edscore-decider:
//! a state (the tool and its arguments), the window before and after the call (inputs `view_before`,
//! `view_after`), and typed Choice questions. A question's gold answer is kept only when the driver
//! measured it: the `effect` question takes the action record's effect, and an `unverifiable` effect
//! leaves the question with no gold, so it never becomes a false label. Real-valued measures (the
//! 8x8 gray change) are kept as measures, not turned into answers. The `decider_row` view yields one
//! row per example and question.
//! Views are PyroWave frames (asset kind `pyrowave`), as XR Pilot keeps its streamed frames: one
//! packet holding the whole 4:2:0 frame, which XR Pilot's decoders read unchanged. The encoder is the
//! `cua-tracer-pyrowave` sidecar next to this executable (Metal on macOS, Vulkan elsewhere), started
//! once per session by the writer thread. There is no PNG fallback: a view the sidecar cannot encode
//! is not kept, and the span stays a plain span.
//!
//! Tracing never fails or delays the action it observes: events go to a bounded queue with
//! `try_send`, and a writer thread owns the database. A full queue drops the event and counts it.
//!
//! On by default. `CUA_DRIVER_TRACE=off` turns it off; `CUA_DRIVER_TRACE=<path>` picks the file.
//! The default file is `traces.sqlite` in the per-user data directory under `V-Sekai-fire/cua-tracer`
//! (XR Pilot's is under `V-Sekai-fire/xr-pilot`). `CUA_DRIVER_TRACE_BPP` sets the PyroWave budget in
//! bits per pixel (default [`DEFAULT_BPP`]); `CUA_DRIVER_TRACE_PYROWAVE` names the sidecar.

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
pub const SCHEMA_VERSION: i64 = 4;
const QUEUE_CAPACITY: usize = 256;
/// PyroWave budget for a view, bits per pixel of the even-padded frame.
pub const DEFAULT_BPP: f64 = 1.0;

/// The interned vocabularies, one relation each (XR Pilot's list).
const VOCABULARIES: &[&str] = &["tool", "task_type", "input_role", "asset_kind", "metric"];

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
CREATE TABLE IF NOT EXISTS question (
    question_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    text TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS question_option (
    question_id INTEGER NOT NULL REFERENCES question,
    option_index INTEGER NOT NULL,
    label TEXT NOT NULL,
    PRIMARY KEY (question_id, option_index),
    UNIQUE (question_id, label));
CREATE TABLE IF NOT EXISTS decider_example (
    span_id INTEGER PRIMARY KEY REFERENCES span,
    task_type_id INTEGER NOT NULL REFERENCES task_type);
CREATE TABLE IF NOT EXISTS decider_input (
    span_id INTEGER NOT NULL REFERENCES decider_example,
    input_role_id INTEGER NOT NULL REFERENCES input_role,
    asset_id INTEGER NOT NULL REFERENCES asset,
    PRIMARY KEY (span_id, input_role_id));
CREATE TABLE IF NOT EXISTS decider_question (
    span_id INTEGER NOT NULL REFERENCES decider_example,
    question_id INTEGER NOT NULL REFERENCES question,
    PRIMARY KEY (span_id, question_id));
CREATE TABLE IF NOT EXISTS decider_gold (
    span_id INTEGER NOT NULL,
    question_id INTEGER NOT NULL,
    option_index INTEGER NOT NULL,
    PRIMARY KEY (span_id, question_id),
    FOREIGN KEY (span_id, question_id) REFERENCES decider_question,
    FOREIGN KEY (question_id, option_index) REFERENCES question_option);
CREATE TABLE IF NOT EXISTS decider_measure (
    span_id INTEGER NOT NULL REFERENCES decider_example,
    metric_id INTEGER NOT NULL REFERENCES metric,
    value REAL NOT NULL,
    PRIMARY KEY (span_id, metric_id));
-- One row per example and question: the state, the question, its options in order, and the gold
-- answer's label where one was measured (gold is NULL here only because the view outer-joins).
CREATE VIEW IF NOT EXISTS decider_row AS
SELECT e.span_id, s.session_id, tt.name AS task_type,
       t.name || COALESCE(' ' || d.detail, '') AS state,
       q.name AS question, q.text AS question_text,
       (SELECT json_group_array(label) FROM
           (SELECT label FROM question_option o WHERE o.question_id = q.question_id ORDER BY option_index))
           AS options,
       go.label AS gold
FROM decider_example e
JOIN span s USING (span_id)
JOIN tool t USING (tool_id)
JOIN task_type tt USING (task_type_id)
LEFT JOIN span_detail d USING (span_id)
JOIN decider_question dq ON dq.span_id = e.span_id
JOIN question q ON q.question_id = dq.question_id
LEFT JOIN decider_gold g ON g.span_id = dq.span_id AND g.question_id = dq.question_id
LEFT JOIN question_option go ON go.question_id = g.question_id AND go.option_index = g.option_index;
"#;

/// The `effect` question: what the action did to the window. Its options are the driver's measured
/// effects; `unverifiable` is not an option, because it says the driver could not tell.
pub const EFFECT_QUESTION: &str = "effect";
const EFFECT_TEXT: &str = "What did the action do to the window?";
pub const EFFECT_OPTIONS: &[&str] = &["confirmed", "partial", "suspected_noop", "refused"];

/// The gold option for an effect, or `None` when the effect is not a measured answer.
pub fn effect_gold(effect: crate::action_record::ActionEffect) -> Option<&'static str> {
    use crate::action_record::ActionEffect::*;
    match effect {
        Confirmed => Some("confirmed"),
        Partial => Some("partial"),
        SuspectedNoop => Some("suspected_noop"),
        Refused => Some("refused"),
        Unverifiable => None,
    }
}

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
        /// The action record's effect, when the tool produced one.
        effect: Option<crate::action_record::ActionEffect>,
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
                Ok(store) => {
                    // Spans still in the queue when the process exits (stdin EOF, process::exit)
                    // would be lost, so drain them at exit. The writer thread is still alive then.
                    unsafe { libc::atexit(flush_at_exit) };
                    Some(store)
                }
                Err(error) => {
                    tracing::warn!("trace store off: cannot open {}: {error:#}", path.display());
                    None
                }
            }
        })
        .as_ref()
}

extern "C" fn flush_at_exit() {
    if let Some(Some(store)) = GLOBAL.get() {
        store.flush();
    }
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
            .spawn(move || Writer::new(conn, session, Box::new(Sidecar::from_env())).run(rx))?;
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

    /// Ends a span; `view` is the target window after the call, `effect` the action record's effect.
    pub fn end(
        &self,
        span: OpenSpan,
        ok: bool,
        view: Option<Vec<u8>>,
        effect: Option<crate::action_record::ActionEffect>,
    ) {
        {
            let mut running = self.running.lock().unwrap_or_else(|e| e.into_inner());
            running.retain(|k| *k != span.key);
        }
        self.send(Event::End {
            key: span.key,
            ok,
            at_ms: self.now_ms(),
            view,
            effect,
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

/// Turns an even-sized RGBA8 frame into one PyroWave packet; `None` when it cannot.
pub trait ViewEncoder: Send {
    fn encode(&mut self, width: u32, height: u32, rgba: &[u8]) -> Option<Vec<u8>>;
}

/// The `cua-tracer-pyrowave` process, started on the first view and restarted after it fails.
pub struct Sidecar {
    path: PathBuf,
    bpp: f64,
    child: Option<(std::process::Child, std::process::ChildStdin, std::process::ChildStdout)>,
    warned: bool,
}

impl Sidecar {
    pub fn from_env() -> Self {
        let name = if cfg!(windows) { "cua-tracer-pyrowave.exe" } else { "cua-tracer-pyrowave" };
        let path = std::env::var_os("CUA_DRIVER_TRACE_PYROWAVE").map(PathBuf::from).unwrap_or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|p| std::fs::canonicalize(p).ok())
                .and_then(|p| p.parent().map(|d| d.join(name)))
                .unwrap_or_else(|| PathBuf::from(name))
        });
        let bpp = std::env::var("CUA_DRIVER_TRACE_BPP")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(DEFAULT_BPP);
        Self { path, bpp, child: None, warned: false }
    }

    fn start(&mut self) -> std::io::Result<()> {
        use std::process::{Command, Stdio};
        let mut child = Command::new(&self.path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdin = child.stdin.take().expect("piped");
        let stdout = child.stdout.take().expect("piped");
        self.child = Some((child, stdin, stdout));
        Ok(())
    }

    fn round_trip(&mut self, width: u32, height: u32, rgba: &[u8]) -> std::io::Result<Vec<u8>> {
        use std::io::{Read, Write};
        if self.child.is_none() {
            self.start()?;
        }
        let (_, stdin, stdout) = self.child.as_mut().expect("started");
        let budget = ((width as f64 * height as f64 * self.bpp / 8.0) as u32).max(1);
        let mut header = Vec::with_capacity(12);
        for v in [width, height, budget] {
            header.extend_from_slice(&v.to_le_bytes());
        }
        stdin.write_all(&header)?;
        stdin.write_all(rgba)?;
        stdin.flush()?;
        let mut len = [0u8; 4];
        stdout.read_exact(&mut len)?;
        let mut packet = vec![0u8; u32::from_le_bytes(len) as usize];
        stdout.read_exact(&mut packet)?;
        Ok(packet)
    }
}

impl ViewEncoder for Sidecar {
    fn encode(&mut self, width: u32, height: u32, rgba: &[u8]) -> Option<Vec<u8>> {
        match self.round_trip(width, height, rgba) {
            Ok(packet) if !packet.is_empty() => Some(packet),
            Ok(_) => None,
            Err(error) => {
                if let Some((mut child, _, _)) = self.child.take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                if !self.warned {
                    self.warned = true;
                    tracing::warn!("trace views off until the sidecar works: {}: {error}", self.path.display());
                }
                None
            }
        }
    }
}

impl Drop for Sidecar {
    fn drop(&mut self) {
        if let Some((mut child, stdin, _)) = self.child.take() {
            drop(stdin); // EOF ends the sidecar's loop
            let _ = child.wait();
        }
    }
}

/// A decoded view: the frame padded to even size for 4:2:0, and the 8x8 gray grid of the original.
struct Frame {
    width: u32,
    height: u32,
    rgba: Vec<u8>,
    thumb: Vec<u8>,
}

fn frame(png: &[u8]) -> Option<Frame> {
    let img = image::load_from_memory_with_format(png, image::ImageFormat::Png).ok()?.to_rgba8();
    let (w, h) = img.dimensions();
    if w < 2 || h < 2 {
        return None;
    }
    let mut thumb = Vec::new();
    let mut y = 0;
    while y + 8 <= h {
        let mut x = 0;
        while x + 8 <= w {
            let p = img.get_pixel(x, y).0;
            thumb.push(((p[0] as u32 + 2 * p[1] as u32 + p[2] as u32) / 4) as u8);
            x += 8;
        }
        y += 8;
    }
    let (pw, ph) = (w + (w & 1), h + (h & 1));
    let mut rgba = vec![0u8; (pw * ph * 4) as usize];
    let src = img.as_raw();
    for row in 0..ph {
        let from = row.min(h - 1) as usize * w as usize * 4; // repeat the edge row and column
        let to = row as usize * pw as usize * 4;
        rgba[to..to + w as usize * 4].copy_from_slice(&src[from..from + w as usize * 4]);
        if pw != w {
            let last = to + (w as usize - 1) * 4;
            let (head, tail) = rgba.split_at_mut(last + 4);
            tail[..4].copy_from_slice(&head[last..last + 4]);
        }
    }
    Some(Frame { width: pw, height: ph, rgba, thumb })
}

struct Writer {
    conn: Connection,
    session: i64,
    spans: HashMap<u64, i64>,
    input_thumb: HashMap<i64, (u32, u32, Vec<u8>)>,
    vocab: HashMap<(&'static str, String), i64>,
    encoder: Box<dyn ViewEncoder>,
}

impl Writer {
    fn new(conn: Connection, session: i64, encoder: Box<dyn ViewEncoder>) -> Self {
        Self {
            conn,
            session,
            spans: HashMap::new(),
            input_thumb: HashMap::new(),
            vocab: HashMap::new(),
            encoder,
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

    /// The id of a Choice question, defining it and its options in order on first use. A store whose
    /// question has other options keeps its definition; the gold insert then finds no matching label.
    fn question(&mut self, name: &str, text: &str, options: &[&str]) -> rusqlite::Result<i64> {
        if let Some(id) = self.vocab.get(&("question", name.to_string())) {
            return Ok(*id);
        }
        self.conn
            .execute("INSERT OR IGNORE INTO question (name, text) VALUES (?1, ?2)", params![name, text])?;
        let id: i64 =
            self.conn.query_row("SELECT question_id FROM question WHERE name = ?1", params![name], |r| r.get(0))?;
        for (i, label) in options.iter().enumerate() {
            self.conn.execute(
                "INSERT OR IGNORE INTO question_option VALUES (?1, ?2, ?3)",
                params![id, i as i64, label],
            )?;
        }
        self.vocab.insert(("question", name.to_string()), id);
        Ok(id)
    }

    /// Stores a view as a PyroWave frame with its encoded extent; `None` when it does not decode or
    /// encode, so no row points at a view that is not there.
    fn asset(&mut self, png: &[u8]) -> rusqlite::Result<Option<(i64, (u32, u32, Vec<u8>))>> {
        let Some(f) = frame(png) else { return Ok(None) };
        let Some(packet) = self.encoder.encode(f.width, f.height, &f.rgba) else { return Ok(None) };
        let kind = self.intern("asset_kind", "pyrowave")?;
        self.conn
            .execute("INSERT INTO asset (asset_kind_id, bytes) VALUES (?1, ?2)", params![kind, packet])?;
        let id = self.conn.last_insert_rowid();
        self.conn
            .execute("INSERT INTO asset_extent VALUES (?1, ?2, ?3)", params![id, f.width, f.height])?;
        Ok(Some((id, (f.width, f.height, f.thumb))))
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
                if let Some((asset, thumb)) = match view {
                    Some(png) => self.asset(&png)?,
                    None => None,
                } {
                    let task = self.intern("task_type", "agent_step")?;
                    let role = self.intern("input_role", "view_before")?;
                    self.conn.execute(
                        "INSERT OR IGNORE INTO decider_example VALUES (?1, ?2)",
                        params![id, task],
                    )?;
                    self.conn.execute(
                        "INSERT OR IGNORE INTO decider_input VALUES (?1, ?2, ?3)",
                        params![id, role, asset],
                    )?;
                    self.input_thumb.insert(id, thumb);
                }
            }
            Event::End { key, ok, at_ms, view, effect } => {
                let Some(id) = self.spans.remove(&key) else {
                    return Ok(());
                };
                self.conn.execute(
                    "INSERT OR REPLACE INTO span_end VALUES (?1, ?2, ?3)",
                    params![id, iso_duration(at_ms), ok as i64],
                )?;
                let example: Option<i64> = self
                    .conn
                    .query_row("SELECT span_id FROM decider_example WHERE span_id = ?1", params![id], |r| {
                        r.get(0)
                    })
                    .optional()?;
                let before = self.input_thumb.remove(&id);
                if example.is_none() {
                    return Ok(());
                }
                if let Some(effect) = effect {
                    let question = self.question(EFFECT_QUESTION, EFFECT_TEXT, EFFECT_OPTIONS)?;
                    self.conn.execute(
                        "INSERT OR IGNORE INTO decider_question VALUES (?1, ?2)",
                        params![id, question],
                    )?;
                    if let Some(label) = effect_gold(effect) {
                        self.conn.execute(
                            "INSERT OR REPLACE INTO decider_gold SELECT ?1, ?2, option_index \
                             FROM question_option WHERE question_id = ?2 AND label = ?3",
                            params![id, question, label],
                        )?;
                    }
                }
                let stored = match view {
                    Some(png) => self.asset(&png)?,
                    None => None,
                };
                if let Some((asset, after)) = stored {
                    let role = self.intern("input_role", "view_after")?;
                    self.conn.execute(
                        "INSERT OR IGNORE INTO decider_input VALUES (?1, ?2, ?3)",
                        params![id, role, asset],
                    )?;
                    if let (Some((bw, bh, b)), (aw, ah, a)) = (before, after) {
                        if (bw, bh) == (aw, ah) && !a.is_empty() {
                            let metric = self.intern("metric", "gray_l1_8x8")?;
                            self.conn.execute(
                                "INSERT OR REPLACE INTO decider_measure VALUES (?1, ?2, ?3)",
                                params![id, metric, gray_l1(&b, &a)],
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

    /// Stands in for the GPU: a "packet" that records the extent and the first pixel.
    struct FakeEncoder;
    impl ViewEncoder for FakeEncoder {
        fn encode(&mut self, w: u32, h: u32, rgba: &[u8]) -> Option<Vec<u8>> {
            assert_eq!(rgba.len(), (w * h * 4) as usize);
            assert!(w % 2 == 0 && h % 2 == 0, "4:2:0 needs even extents");
            Some([&w.to_le_bytes()[..], &h.to_le_bytes()[..], &rgba[..4]].concat())
        }
    }
    struct FailingEncoder;
    impl ViewEncoder for FailingEncoder {
        fn encode(&mut self, _: u32, _: u32, _: &[u8]) -> Option<Vec<u8>> {
            None
        }
    }

    fn open_with(path: &Path, encoder: Box<dyn ViewEncoder>) -> TraceStore {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).unwrap();
        }
        let conn = open_schema(path).unwrap();
        conn.execute("INSERT INTO session (started_at) VALUES ('t')", []).unwrap();
        let session = conn.last_insert_rowid();
        let (tx, rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        thread::spawn(move || Writer::new(conn, session, encoder).run(rx));
        TraceStore {
            tx,
            epoch: Instant::now(),
            next_key: AtomicU64::new(1),
            running: Mutex::new(Vec::new()),
            dropped: AtomicU64::new(0),
            path: path.to_path_buf(),
        }
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

    use crate::action_record::ActionEffect;

    fn gold(conn: &Connection) -> Vec<(String, String, Option<String>)> {
        conn.prepare("SELECT question, options, gold FROM decider_row ORDER BY span_id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn spans_nest_end_and_form_a_decider_example() {
        let path = temp_db("pair");
        let store = open_with(&path, Box::new(FakeEncoder));
        let outer = store.begin("run_steps", &json!({}), None);
        let inner = store.begin("click", &json!({"pid": 1, "x": 3}), Some(png(64, 32, 10)));
        store.end(inner, true, Some(png(64, 32, 200)), Some(ActionEffect::Confirmed));
        store.end(outer, false, None, None);
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
        let state: String = conn.query_row("SELECT DISTINCT state FROM decider_row", [], |r| r.get(0)).unwrap();
        assert_eq!(state, r#"click {"pid":1,"x":3}"#);
        assert_eq!(
            gold(&conn),
            vec![(
                "effect".to_string(),
                r#"["confirmed","partial","suspected_noop","refused"]"#.to_string(),
                Some("confirmed".to_string())
            )]
        );
        let roles: Vec<String> = conn
            .prepare("SELECT r.name FROM decider_input JOIN input_role r USING (input_role_id) ORDER BY r.name")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(roles, vec!["view_after".to_string(), "view_before".to_string()]);
        let score: f64 = conn.query_row("SELECT value FROM decider_measure", [], |r| r.get(0)).unwrap();
        // Views are PyroWave frames at the even-padded extent, never PNG.
        let kinds: Vec<String> = conn
            .prepare("SELECT DISTINCT k.name FROM asset JOIN asset_kind k USING (asset_kind_id)")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(kinds, vec!["pyrowave".to_string()]);
        assert!((score - 190.0 / 255.0).abs() < 1e-9, "score {score}");
        // The ETNF rule XR Pilot checks: no column anywhere holds a NULL.
        for table in ["span", "span_detail", "span_parent", "span_end", "asset", "asset_extent", "question",
            "question_option", "decider_example", "decider_input", "decider_question", "decider_gold",
            "decider_measure"] {
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
    fn an_unverifiable_effect_asks_the_question_and_keeps_no_gold() {
        let path = temp_db("unverifiable");
        let store = open_with(&path, Box::new(FakeEncoder));
        let s = store.begin("click", &json!({"pid": 1}), Some(png(64, 32, 10)));
        store.end(s, true, Some(png(64, 32, 10)), Some(ActionEffect::Unverifiable));
        // A control: the same call with a measured effect does get its gold.
        let c = store.begin("click", &json!({"pid": 2}), Some(png(64, 32, 10)));
        store.end(c, true, Some(png(64, 32, 10)), Some(ActionEffect::SuspectedNoop));
        // A tool with no action record asks no question.
        let n = store.begin("get_window_state", &json!({"pid": 3}), Some(png(64, 32, 10)));
        store.end(n, true, Some(png(64, 32, 10)), None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let rows = gold(&conn);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].2, None);
        assert_eq!(rows[1].2.as_deref(), Some("suspected_noop"));
        let golds: i64 = conn.query_row("SELECT count(*) FROM decider_gold", [], |r| r.get(0)).unwrap();
        assert_eq!(golds, 1);
        let examples: i64 = conn.query_row("SELECT count(*) FROM decider_example", [], |r| r.get(0)).unwrap();
        assert_eq!(examples, 3);
    }

    #[test]
    fn every_effect_but_unverifiable_is_an_option() {
        for e in [ActionEffect::Confirmed, ActionEffect::Partial, ActionEffect::SuspectedNoop, ActionEffect::Refused] {
            assert!(EFFECT_OPTIONS.contains(&effect_gold(e).unwrap()), "{e:?}");
        }
        assert_eq!(effect_gold(ActionEffect::Unverifiable), None);
    }

    #[test]
    fn odd_windows_are_padded_and_a_failed_encode_keeps_no_view() {
        let path = temp_db("odd");
        let store = open_with(&path, Box::new(FakeEncoder));
        let s = store.begin("click", &json!({"pid": 1}), Some(png(673, 439, 50)));
        store.end(s, true, Some(png(673, 439, 60)), None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let extent: (i64, i64) =
            conn.query_row("SELECT width, height FROM asset_extent LIMIT 1", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        assert_eq!(extent, (674, 440));

        // A control: the same calls with an encoder that fails leave plain spans and no assets.
        let path = temp_db("fail");
        let store = open_with(&path, Box::new(FailingEncoder));
        let s = store.begin("click", &json!({"pid": 1}), Some(png(64, 32, 10)));
        store.end(s, true, Some(png(64, 32, 200)), None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let counts: (i64, i64, i64) = conn
            .query_row(
                "SELECT (SELECT count(*) FROM asset), (SELECT count(*) FROM decider_example), (SELECT count(*) FROM span_end)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(counts, (0, 0, 1));
    }

    #[test]
    fn a_span_without_a_view_is_not_a_decider_example() {
        let path = temp_db("noview");
        let store = open_with(&path, Box::new(FakeEncoder));
        let s = store.begin("list_windows", &json!({}), None);
        store.end(s, true, None, None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let roots: i64 = conn.query_row("SELECT count(*) FROM decider_example", [], |r| r.get(0)).unwrap();
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
            c.execute_batch("CREATE TABLE old (x); PRAGMA user_version = 3;").unwrap();
        }
        let _store = TraceStore::open(&path).unwrap();
        assert!(PathBuf::from(format!("{}.v3", path.display())).exists());
    }
}
