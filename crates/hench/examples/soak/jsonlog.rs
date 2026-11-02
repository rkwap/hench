//! Append-only JSON-lines logs, outside Redis. Every record is written with one `write(2)`
//! (no user-space buffering), so a `kill -9` can lose at most the record being formatted,
//! never a record already written.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// An append-only JSON-lines file.
pub struct JsonLog {
    file: Mutex<File>,
}

impl JsonLog {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// Appends one record (a JSON object). Errors are printed, never fatal.
    pub fn write(&self, value: &Value) {
        let mut line = match serde_json::to_vec(value) {
            Ok(line) => line,
            Err(error) => {
                eprintln!("jsonlog: cannot encode {value:?}: {error}");
                return;
            }
        };
        line.push(b'\n');
        let mut file = self.file.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(error) = file.write_all(&line) {
            eprintln!("jsonlog: cannot write: {error}");
        }
    }
}

/// Reads every complete JSON record of a JSON-lines file (a torn last line is skipped).
pub fn read_all(path: &Path) -> anyhow::Result<Vec<Value>> {
    let file = File::open(path)?;
    let mut out = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(value) => out.push(value),
            Err(error) => eprintln!("{}: skipping a torn line: {error}", path.display()),
        }
    }
    Ok(out)
}
