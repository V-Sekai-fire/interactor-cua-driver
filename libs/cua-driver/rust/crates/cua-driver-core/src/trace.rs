//! Every tool call as a span, kept in SQLite in the schema XR Pilot's `TraceStore` writes
//! (V-Sekai-fire/interactor-xr-pilot `core/src/TraceStore.cpp`, schema version 5), so the same readers
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
//!
//! Views are lossless WebP (RFC 9649, asset kind `webp`): the window's 8-bit sRGB pixels
//! exactly, tagged sRGB, at the window's own size, as XR Pilot keeps its frames. WebP's bitstream
//! is frozen, so a stored view stays readable. The writer thread encodes it in process. A view that
//! does not decode or encode is not kept, and the span stays a plain span.
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
pub const SCHEMA_VERSION: i64 = 5;
const QUEUE_CAPACITY: usize = 256;

/// The interned vocabularies, one relation each (XR Pilot's list).
const VOCABULARIES: &[&str] = &["tool", "task_type", "input_role", "asset_kind", "metric"];

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS session (
    session_id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
    started_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS span (
    span_id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
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
    uuid TEXT NOT NULL UNIQUE,
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
    /// Write what is queued, close the database, and stop the writer.
    Close(SyncSender<()>),
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
    /// The writer thread, joined by [`TraceStore::close`].
    writer: Mutex<Option<thread::JoinHandle<()>>>,
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
                _ => dirs::data_dir()?
                    .join("V-Sekai-fire")
                    .join("cua-tracer")
                    .join("traces.sqlite"),
            };
            match TraceStore::open(&path) {
                Ok(store) => {
                    // Spans still in the queue when the process exits (stdin EOF, process::exit)
                    // would be lost, so write them at exit, then stop the writer: it closes the
                    // database and frees what it holds before the process goes.
                    unsafe { libc::atexit(close_at_exit) };
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

extern "C" fn close_at_exit() {
    if let Some(Some(store)) = GLOBAL.get() {
        store.close();
    }
}

impl TraceStore {
    /// Creates the schema if needed and starts a session.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn = open_schema(path)?;
        let epoch = Instant::now();
        let started_ms = unix_ms();
        conn.execute(
            "INSERT INTO session (uuid, started_at) VALUES (?1, ?2)",
            params![uuid7(started_ms), civil_time(started_ms)],
        )?;
        let session = conn.last_insert_rowid();
        let (tx, rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        let writer = thread::Builder::new()
            .name("cua-trace-writer".into())
            .spawn(move || Writer::new(conn, session, started_ms, Box::new(Webp)).run(rx))?;
        Ok(Self {
            tx,
            epoch,
            next_key: AtomicU64::new(1),
            running: Mutex::new(Vec::new()),
            dropped: AtomicU64::new(0),
            path: path.to_path_buf(),
            writer: Mutex::new(Some(writer)),
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

    /// Writes every queued event, closes the database and joins the writer thread. Later events are
    /// counted as dropped. Called at process exit; calling it again does nothing.
    pub fn close(&self) {
        let Some(writer) = self.writer.lock().unwrap_or_else(|e| e.into_inner()).take() else {
            return;
        };
        let (done, wait) = mpsc::sync_channel(1);
        // A blocking send: the close must not be dropped when the queue is full.
        if self.tx.send(Event::Close(done)).is_ok() {
            let _ = wait.recv();
        }
        let _ = writer.join();
    }
}

fn open_schema(path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(std::time::Duration::from_millis(2000))?;
    let have: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    let tables: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table'",
        [],
        |r| r.get(0),
    )?;
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
                            (true, Value::String(s)) => {
                                Value::String(format!("<{} chars>", s.chars().count()))
                            }
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

/// Turns an RGB8 frame (rows top to bottom, no padding) into one stored view; `None` when it cannot.
pub trait ViewEncoder: Send {
    fn encode(&mut self, width: u32, height: u32, rgb: &[u8]) -> Option<Vec<u8>>;
}

/// Lossless WebP (RFC 9649, VP8L): the 8-bit sRGB pixels exactly, from libwebp at method 0, the
/// same settings XR Pilot's store uses, fast enough to keep up with a trace on one thread.
pub struct Webp;

impl ViewEncoder for Webp {
    fn encode(&mut self, width: u32, height: u32, rgb: &[u8]) -> Option<Vec<u8>> {
        if width == 0 || height == 0 || rgb.len() != (width as usize) * (height as usize) * 3 {
            return None;
        }
        let mut config = webp::WebPConfig::new().ok()?;
        config.lossless = 1;
        config.exact = 1;
        config.method = 0;
        config.quality = 0.0;
        match webp::Encoder::from_rgb(rgb, width, height).encode_advanced(&config) {
            Ok(memory) if !memory.is_empty() => Some(memory.to_vec()),
            Ok(_) => None,
            Err(error) => {
                tracing::warn!("trace view not kept: WebP encode failed: {error:?}");
                None
            }
        }
    }
}

/// A decoded view: its RGB8 pixels, and the 8x8 gray grid a span's measure compares.
struct Frame {
    width: u32,
    height: u32,
    rgb: Vec<u8>,
    thumb: Vec<u8>,
}

fn frame(png: &[u8]) -> Option<Frame> {
    let img = image::load_from_memory_with_format(png, image::ImageFormat::Png)
        .ok()?
        .to_rgb8();
    let (width, height) = img.dimensions();
    if width == 0 || height == 0 {
        return None;
    }
    let mut thumb = Vec::new();
    let mut y = 0;
    while y + 8 <= height {
        let mut x = 0;
        while x + 8 <= width {
            let p = img.get_pixel(x, y).0;
            thumb.push(((p[0] as u32 + 2 * p[1] as u32 + p[2] as u32) / 4) as u8);
            x += 8;
        }
        y += 8;
    }
    Some(Frame {
        width,
        height,
        rgb: img.into_raw(),
        thumb,
    })
}

struct Writer {
    conn: Connection,
    session: i64,
    /// The session's start in Unix milliseconds: a row's UUIDv7 time is this plus the row's offset.
    started_ms: i64,
    spans: HashMap<u64, i64>,
    input_thumb: HashMap<i64, (u32, u32, Vec<u8>)>,
    vocab: HashMap<(&'static str, String), i64>,
    encoder: Box<dyn ViewEncoder>,
}

impl Writer {
    fn new(conn: Connection, session: i64, started_ms: i64, encoder: Box<dyn ViewEncoder>) -> Self {
        Self {
            conn,
            session,
            started_ms,
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
                Event::Close(done) => {
                    // Events queued before the close are already written: the queue is in order.
                    drop(self);
                    let _ = done.send(());
                    return;
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
        self.conn.execute(
            &format!("INSERT OR IGNORE INTO {table} (name) VALUES (?1)"),
            params![name],
        )?;
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
        self.conn.execute(
            "INSERT OR IGNORE INTO question (name, text) VALUES (?1, ?2)",
            params![name, text],
        )?;
        let id: i64 = self.conn.query_row(
            "SELECT question_id FROM question WHERE name = ?1",
            params![name],
            |r| r.get(0),
        )?;
        for (i, label) in options.iter().enumerate() {
            self.conn.execute(
                "INSERT OR IGNORE INTO question_option VALUES (?1, ?2, ?3)",
                params![id, i as i64, label],
            )?;
        }
        self.vocab.insert(("question", name.to_string()), id);
        Ok(id)
    }

    /// Stores a view as lossless WebP with its extent; `None` when it does not decode or encode,
    /// so no row points at a view that is not there.
    /// `at_ms` is the moment the view shows, as an offset into the session.
    fn asset(
        &mut self,
        png: &[u8],
        at_ms: i64,
    ) -> rusqlite::Result<Option<(i64, (u32, u32, Vec<u8>))>> {
        let Some(f) = frame(png) else { return Ok(None) };
        let Some(packet) = self.encoder.encode(f.width, f.height, &f.rgb) else {
            return Ok(None);
        };
        let kind = self.intern("asset_kind", "webp")?;
        self.conn.execute(
            "INSERT INTO asset (uuid, asset_kind_id, bytes) VALUES (?1, ?2, ?3)",
            params![uuid7(self.started_ms + at_ms), kind, packet],
        )?;
        let id = self.conn.last_insert_rowid();
        self.conn.execute(
            "INSERT INTO asset_extent VALUES (?1, ?2, ?3)",
            params![id, f.width, f.height],
        )?;
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
            Event::Begin {
                key,
                parent,
                tool,
                detail,
                at_ms,
                view,
            } => {
                let tool_id = self.intern("tool", &tool)?;
                self.conn.execute(
                    "INSERT INTO span (uuid, session_id, span_key, tool_id, started) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![uuid7(self.started_ms + at_ms), self.session, key.to_string(), tool_id, iso_duration(at_ms)],
                )?;
                let id = self.conn.last_insert_rowid();
                self.spans.insert(key, id);
                if !detail.is_empty() {
                    self.conn.execute(
                        "INSERT INTO span_detail VALUES (?1, ?2)",
                        params![id, detail],
                    )?;
                }
                if let Some(parent) = parent.and_then(|p| self.spans.get(&p).copied()) {
                    self.conn.execute(
                        "INSERT INTO span_parent VALUES (?1, ?2)",
                        params![id, parent],
                    )?;
                }
                if let Some((asset, thumb)) = match view {
                    Some(png) => self.asset(&png, at_ms)?,
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
            Event::End {
                key,
                ok,
                at_ms,
                view,
                effect,
            } => {
                let Some(id) = self.spans.remove(&key) else {
                    return Ok(());
                };
                self.conn.execute(
                    "INSERT OR REPLACE INTO span_end VALUES (?1, ?2, ?3)",
                    params![id, iso_duration(at_ms), ok as i64],
                )?;
                let example: Option<i64> = self
                    .conn
                    .query_row(
                        "SELECT span_id FROM decider_example WHERE span_id = ?1",
                        params![id],
                        |r| r.get(0),
                    )
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
                    Some(png) => self.asset(&png, at_ms)?,
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
            Event::Flush(_) | Event::Close(_) => {}
        }
        Ok(())
    }
}

/// Mean absolute difference of two thumbs, 0..1.
fn gray_l1(a: &[u8], b: &[u8]) -> f64 {
    let sum: u64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| (*x as i32 - *y as i32).unsigned_abs() as u64)
        .sum();
    sum as f64 / a.len() as f64 / 255.0
}

/// A UUIDv7 (RFC 9562) whose time is `unix_ms`: every session, span and view is named by one,
/// timed to the moment it describes, so rows from many stores merge without collisions.
pub fn uuid7(unix_ms: i64) -> String {
    let ms = unix_ms.max(0) as u64;
    let ts =
        uuid::Timestamp::from_unix(uuid::NoContext, ms / 1000, ((ms % 1000) * 1_000_000) as u32);
    uuid::Uuid::new_v7(ts).to_string()
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// RECTGTN's offset form, as XR Pilot writes it: `PT1M2.5S`.
pub fn iso_duration(ms: i64) -> String {
    let ms = ms.max(0);
    let (hours, minutes, seconds, millis) =
        (ms / 3_600_000, ms / 60_000 % 60, ms / 1000 % 60, ms % 1000);
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

    /// Stands in for the encoder: a "view" that records the extent and the first pixel.
    struct FakeEncoder;
    impl ViewEncoder for FakeEncoder {
        fn encode(&mut self, w: u32, h: u32, rgb: &[u8]) -> Option<Vec<u8>> {
            assert_eq!(rgb.len(), (w * h * 3) as usize);
            Some([&w.to_le_bytes()[..], &h.to_le_bytes()[..], &rgb[..3]].concat())
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
        let started_ms = unix_ms();
        conn.execute(
            "INSERT INTO session (uuid, started_at) VALUES (?1, ?2)",
            params![uuid7(started_ms), civil_time(started_ms)],
        )
        .unwrap();
        let session = conn.last_insert_rowid();
        let (tx, rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        let writer = thread::spawn(move || Writer::new(conn, session, started_ms, encoder).run(rx));
        TraceStore {
            tx,
            epoch: Instant::now(),
            next_key: AtomicU64::new(1),
            running: Mutex::new(Vec::new()),
            dropped: AtomicU64::new(0),
            path: path.to_path_buf(),
            writer: Mutex::new(Some(writer)),
        }
    }

    fn temp_db(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("cua-trace-test-{}-{name}", std::process::id()));
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
        store.end(
            inner,
            true,
            Some(png(64, 32, 200)),
            Some(ActionEffect::Confirmed),
        );
        store.end(outer, false, None, None);
        store.flush();

        let conn = Connection::open(&path).unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let spans: i64 = conn
            .query_row("SELECT count(*) FROM span", [], |r| r.get(0))
            .unwrap();
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
        assert_eq!(
            (child_tool.as_str(), parent_tool.as_str()),
            ("click", "run_steps")
        );
        let ends: Vec<i64> = conn
            .prepare("SELECT ok FROM span_end ORDER BY span_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(ends, vec![0, 1]);
        let state: String = conn
            .query_row("SELECT DISTINCT state FROM decider_row", [], |r| r.get(0))
            .unwrap();
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
        assert_eq!(
            roles,
            vec!["view_after".to_string(), "view_before".to_string()]
        );
        let score: f64 = conn
            .query_row("SELECT value FROM decider_measure", [], |r| r.get(0))
            .unwrap();
        // Views are WebP, never PNG.
        let kinds: Vec<String> = conn
            .prepare("SELECT DISTINCT k.name FROM asset JOIN asset_kind k USING (asset_kind_id)")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(kinds, vec!["webp".to_string()]);
        assert!((score - 190.0 / 255.0).abs() < 1e-9, "score {score}");
        // The ETNF rule XR Pilot checks: no column anywhere holds a NULL.
        for table in [
            "span",
            "span_detail",
            "span_parent",
            "span_end",
            "asset",
            "asset_extent",
            "question",
            "question_option",
            "decider_example",
            "decider_input",
            "decider_question",
            "decider_gold",
            "decider_measure",
        ] {
            let cols: Vec<String> = conn
                .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            for c in cols {
                let nulls: i64 = conn
                    .query_row(
                        &format!("SELECT count(*) FROM {table} WHERE {c} IS NULL"),
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(nulls, 0, "{table}.{c}");
            }
        }
    }

    #[test]
    fn an_unverifiable_effect_asks_the_question_and_keeps_no_gold() {
        let path = temp_db("unverifiable");
        let store = open_with(&path, Box::new(FakeEncoder));
        let s = store.begin("click", &json!({"pid": 1}), Some(png(64, 32, 10)));
        store.end(
            s,
            true,
            Some(png(64, 32, 10)),
            Some(ActionEffect::Unverifiable),
        );
        // A control: the same call with a measured effect does get its gold.
        let c = store.begin("click", &json!({"pid": 2}), Some(png(64, 32, 10)));
        store.end(
            c,
            true,
            Some(png(64, 32, 10)),
            Some(ActionEffect::SuspectedNoop),
        );
        // A tool with no action record asks no question.
        let n = store.begin(
            "get_window_state",
            &json!({"pid": 3}),
            Some(png(64, 32, 10)),
        );
        store.end(n, true, Some(png(64, 32, 10)), None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let rows = gold(&conn);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].2, None);
        assert_eq!(rows[1].2.as_deref(), Some("suspected_noop"));
        let golds: i64 = conn
            .query_row("SELECT count(*) FROM decider_gold", [], |r| r.get(0))
            .unwrap();
        assert_eq!(golds, 1);
        let examples: i64 = conn
            .query_row("SELECT count(*) FROM decider_example", [], |r| r.get(0))
            .unwrap();
        assert_eq!(examples, 3);
    }

    #[test]
    fn every_effect_but_unverifiable_is_an_option() {
        for e in [
            ActionEffect::Confirmed,
            ActionEffect::Partial,
            ActionEffect::SuspectedNoop,
            ActionEffect::Refused,
        ] {
            assert!(EFFECT_OPTIONS.contains(&effect_gold(e).unwrap()), "{e:?}");
        }
        assert_eq!(effect_gold(ActionEffect::Unverifiable), None);
    }

    #[test]
    fn odd_windows_keep_their_size_and_a_failed_encode_keeps_no_view() {
        let path = temp_db("odd");
        let store = open_with(&path, Box::new(FakeEncoder));
        let s = store.begin("click", &json!({"pid": 1}), Some(png(673, 439, 50)));
        store.end(s, true, Some(png(673, 439, 60)), None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let extent: (i64, i64) = conn
            .query_row("SELECT width, height FROM asset_extent LIMIT 1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(extent, (673, 439));

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

    /// The pixels of an image that is not flat: every pixel differs from its neighbours.
    fn noise_png(w: u32, h: u32) -> (Vec<u8>, Vec<u8>) {
        let img = image::RgbImage::from_fn(w, h, |x, y| {
            let v = x
                .wrapping_mul(2654435761)
                .wrapping_add(y.wrapping_mul(40503));
            image::Rgb([v as u8, (v >> 8) as u8, (v >> 16) as u8])
        });
        let mut out = std::io::Cursor::new(Vec::new());
        img.write_to(&mut out, image::ImageFormat::Png).unwrap();
        (out.into_inner(), img.into_raw())
    }

    fn webp_decode(bytes: &[u8]) -> (u32, u32, Vec<u8>) {
        let mut decoder = image_webp::WebPDecoder::new(std::io::Cursor::new(bytes)).unwrap();
        let (w, h) = decoder.dimensions();
        let mut rgb = vec![0u8; decoder.output_buffer_size().unwrap()];
        decoder.read_image(&mut rgb).unwrap();
        if decoder.has_alpha() {
            rgb = rgb.chunks_exact(4).flat_map(|p| [p[0], p[1], p[2]]).collect();
        }
        (w, h, rgb)
    }

    #[test]
    fn a_stored_view_decodes_to_the_same_pixels() {
        let path = temp_db("webp");
        let store = open_with(&path, Box::new(Webp));
        let (before, before_rgb) = noise_png(673, 439);
        let (after, _) = noise_png(673, 439);
        let s = store.begin("click", &json!({"pid": 1}), Some(before));
        store.end(s, true, Some(after), None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let bytes: Vec<u8> = conn
            .query_row(
                "SELECT a.bytes FROM decider_input i JOIN input_role r USING (input_role_id) \
                 JOIN asset a ON a.asset_id = i.asset_id WHERE r.name = 'view_before'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        // A WebP file is a RIFF container: "RIFF", a size, "WEBP", then a VP8L (lossless) chunk.
        assert_eq!(&bytes[..4], b"RIFF");
        assert_eq!(&bytes[8..12], b"WEBP");
        assert_eq!(&bytes[12..16], b"VP8L");
        let (w, h, rgb) = webp_decode(&bytes);
        assert_eq!((w, h), (673, 439));
        assert_eq!(rgb.len(), before_rgb.len());
        assert!(rgb == before_rgb, "lossless: every pixel comes back");
        // A control: one changed bit is seen, so the equality above can fail.
        let mut other = before_rgb.clone();
        other[1000] ^= 1;
        assert!(rgb != other);
    }

    /// The Unix milliseconds a UUIDv7 carries, or `None` for a string that is not one.
    fn v7_ms(text: &str) -> Option<i64> {
        let u = uuid::Uuid::parse_str(text).ok()?;
        (u.get_version_num() == 7 && u.get_variant() == uuid::Variant::RFC4122).then(|| {
            let (secs, nanos) = u.get_timestamp()?.to_unix();
            Some(secs as i64 * 1000 + nanos as i64 / 1_000_000)
        })?
    }

    #[test]
    fn sessions_spans_and_views_are_named_by_uuid7_at_their_moment() {
        let path = temp_db("uuid7");
        let store = open_with(&path, Box::new(FakeEncoder));
        let s = store.begin("click", &json!({"pid": 1}), Some(png(64, 32, 10)));
        store.end(s, true, Some(png(64, 32, 200)), None);
        let t = store.begin("click", &json!({"pid": 2}), Some(png(64, 32, 10)));
        store.end(t, true, Some(png(64, 32, 200)), None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let rows = |sql: &str| -> Vec<(String, String)> {
            let mut st = conn.prepare(sql).unwrap();
            st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        let session = rows("SELECT uuid, started_at FROM session");
        let start = time::OffsetDateTime::parse(
            &session[0].1,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let start_ms = (start.unix_timestamp_nanos() / 1_000_000) as i64;
        assert_eq!(
            v7_ms(&session[0].0),
            Some(start_ms),
            "the session's UUIDv7 is its start"
        );
        let offset = |d: &str| -> i64 {
            let secs: f64 = d
                .trim_start_matches("PT")
                .trim_end_matches('S')
                .parse()
                .unwrap();
            (secs * 1000.0).round() as i64
        };
        let spans = rows("SELECT uuid, started FROM span");
        let views = rows(
            "SELECT a.uuid, CASE r.name WHEN 'view_after' THEN e.ended ELSE s.started END \
             FROM decider_input i JOIN input_role r USING (input_role_id) JOIN asset a ON a.asset_id = i.asset_id \
             JOIN span s ON s.span_id = i.span_id JOIN span_end e ON e.span_id = s.span_id",
        );
        assert_eq!((spans.len(), views.len()), (2, 4));
        for (uuid, at) in spans.iter().chain(views.iter()) {
            assert_eq!(
                v7_ms(uuid),
                Some(start_ms + offset(at)),
                "{uuid} is timed to its row ({at})"
            );
        }
        let mut all: Vec<&String> = session
            .iter()
            .chain(&spans)
            .chain(&views)
            .map(|(u, _)| u)
            .collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 7, "every name is distinct");
        // Controls: a v4 UUID and a non-UUID are refused, so the check above can fail.
        assert_eq!(v7_ms(&uuid::Uuid::new_v4().to_string()), None);
        assert_eq!(v7_ms("u"), None);
    }

    /// Writes a small real store for the decider tools' CI check (`tools/decider/`): WebP views,
    /// one confirmed example, one unverifiable, one with no view. Run with `--ignored` and
    /// `CUA_TRACE_FIXTURE=<path>`.
    #[test]
    #[ignore]
    fn writes_a_fixture_store_for_the_decider_tools() {
        let path = PathBuf::from(std::env::var("CUA_TRACE_FIXTURE").expect("CUA_TRACE_FIXTURE"));
        let _ = std::fs::remove_file(&path);
        let store = open_with(&path, Box::new(Webp));
        let a = store.begin(
            "set_value",
            &json!({"pid": 1, "value": "abc"}),
            Some(png(96, 64, 30)),
        );
        store.end(
            a,
            true,
            Some(png(96, 64, 220)),
            Some(ActionEffect::Confirmed),
        );
        let b = store.begin("click", &json!({"pid": 1, "x": 3}), Some(png(96, 64, 30)));
        store.end(
            b,
            true,
            Some(png(96, 64, 30)),
            Some(ActionEffect::Unverifiable),
        );
        let c = store.begin("list_windows", &json!({}), None);
        store.end(c, true, None, None);
        store.flush();
    }

    #[test]
    fn close_writes_the_queue_stops_the_writer_and_drops_later_events() {
        let path = temp_db("close");
        let store = open_with(&path, Box::new(FakeEncoder));
        for i in 0..50 {
            let s = store.begin("click", &json!({ "pid": i }), None);
            store.end(s, true, None, None);
        }
        store.close();
        assert!(
            store.writer.lock().unwrap().is_none(),
            "the writer is joined"
        );
        let conn = Connection::open(&path).unwrap();
        let ended: i64 = conn
            .query_row("SELECT count(*) FROM span_end", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ended, 50, "every span queued before the close is written");
        // After the close nothing more is written, and the events are counted.
        let late = store.begin("click", &json!({}), None);
        store.end(late, true, None, None);
        assert_eq!(store.dropped(), 2);
        store.close();
        let spans: i64 = conn
            .query_row("SELECT count(*) FROM span", [], |r| r.get(0))
            .unwrap();
        assert_eq!(spans, 50);
    }

    #[test]
    fn a_span_without_a_view_is_not_a_decider_example() {
        let path = temp_db("noview");
        let store = open_with(&path, Box::new(FakeEncoder));
        let s = store.begin("list_windows", &json!({}), None);
        store.end(s, true, None, None);
        store.flush();
        let conn = Connection::open(&path).unwrap();
        let roots: i64 = conn
            .query_row("SELECT count(*) FROM decider_example", [], |r| r.get(0))
            .unwrap();
        assert_eq!(roots, 0);
        let details: i64 = conn
            .query_row("SELECT count(*) FROM span_detail", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            details, 0,
            "empty arguments are a missing row, not an empty string"
        );
    }

    #[test]
    fn an_older_store_is_moved_aside() {
        let path = temp_db("aside");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        {
            let c = Connection::open(&path).unwrap();
            c.execute_batch("CREATE TABLE old (x); PRAGMA user_version = 3;")
                .unwrap();
        }
        let _store = TraceStore::open(&path).unwrap();
        assert!(PathBuf::from(format!("{}.v3", path.display())).exists());
    }
}
