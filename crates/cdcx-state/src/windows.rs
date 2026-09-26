//! Tumbling windows over event time (M7).
//!
//! Rows are assigned to fixed, non-overlapping `[start, start + size)`
//! buckets by a timestamp column. Each window is itself a Z-set, so
//! aggregates over a window are just [`crate::zset::sum_by`] /
//! [`crate::zset::count_by`] applied to its contents. Windows close
//! (stop accepting rows) when the watermark passes their end.
//!
//! Event time comes from the row's timestamp column, not the wall
//! clock, so out-of-order changes (replayed transactions after a
//! crash) land in the correct window.

use std::collections::HashMap;

use cdcx_model::Value;

use crate::zset::{Row, ZSet};

/// A tumbling-window operator over one timestamp column.
pub struct TumblingWindows {
    /// Window length in milliseconds.
    size_ms: i64,
    /// Name of the timestamp column.
    timestamp_column: String,
    /// Buckets keyed by window start (ms since epoch).
    windows: HashMap<i64, ZSet>,
    /// Watermark: window ends <= this are closed. Late rows for closed
    /// windows are dropped (counted in `dropped_late`).
    watermark_ms: i64,
    /// Rows dropped for arriving after their window closed.
    pub dropped_late: u64,
}

impl TumblingWindows {
    /// New operator with `size_ms` windows over `timestamp_column`.
    pub fn new(size_ms: i64, timestamp_column: impl Into<String>) -> Self {
        Self {
            size_ms,
            timestamp_column: timestamp_column.into(),
            windows: HashMap::new(),
            watermark_ms: i64::MIN,
            dropped_late: 0,
        }
    }

    /// Window start for an epoch-milliseconds timestamp.
    fn window_start(&self, ts_ms: i64) -> i64 {
        ts_ms.div_euclid(self.size_ms) * self.size_ms
    }

    /// Extract epoch milliseconds from a row's timestamp column.
    ///
    /// Accepts `Value::Timestamp` in RFC 3339 / ISO-8601 (the shape
    /// PostgreSQL emits) via a lenient manual parse, and `Value::Int`
    /// already in epoch milliseconds. Returns `None` for anything else;
    /// such rows cannot be windowed.
    fn ts_of(&self, row: &Row) -> Option<i64> {
        let v = row
            .iter()
            .find(|(n, _)| *n == self.timestamp_column)
            .map(|(_, v)| v)?;
        match v {
            Value::Int(ms) => Some(*ms),
            Value::Timestamp(s) => parse_iso_to_ms(s),
            _ => None,
        }
    }

    /// Apply a change delta to the windows.
    ///
    /// Rows whose window has already closed (per the watermark) are
    /// dropped and counted; this is the standard trade-off — a fully
    /// correct late handler would reopen the window and retract
    /// downstream aggregates, which M7 defers.
    pub fn apply(&mut self, delta: &ZSet) {
        for (row, weight) in delta.iter() {
            let Some(ts) = self.ts_of(row) else {
                continue;
            };
            let start = self.window_start(ts);
            if start + self.size_ms <= self.watermark_ms {
                self.dropped_late += weight.unsigned_abs();
                continue;
            }
            let window = self.windows.entry(start).or_default();
            window.add(row.clone(), weight);
            window.compact();
        }
    }

    /// Advance the watermark. Closes (removes state for) windows that
    /// ended before it; their final contents are returned so downstream
    /// can emit results.
    pub fn advance_watermark(&mut self, watermark_ms: i64) -> Vec<(i64, ZSet)> {
        self.watermark_ms = self.watermark_ms.max(watermark_ms);
        let mut closed = Vec::new();
        let ends: Vec<i64> = self
            .windows
            .keys()
            .filter(|&&start| start + self.size_ms <= watermark_ms)
            .copied()
            .collect();
        for start in ends {
            if let Some(z) = self.windows.remove(&start) {
                closed.push((start, z));
            }
        }
        closed
    }

    /// Iterate open windows: (start_ms, contents).
    pub fn open_windows(&self) -> impl Iterator<Item = (&i64, &ZSet)> {
        self.windows.iter()
    }

    /// Serialize state for checkpointing: window starts + row/weight
    /// pairs. Values are JSON-encoded.
    pub fn state(&self) -> serde_json::Value {
        serde_json::json!({
            "size_ms": self.size_ms,
            "column": self.timestamp_column,
            "watermark_ms": if self.watermark_ms == i64::MIN { serde_json::Value::Null } else { serde_json::json!(self.watermark_ms) },
            "dropped_late": self.dropped_late,
            "windows": self.windows.iter().map(|(start, z)| {
                (
                    start.to_string(),
                    z.iter().map(|(row, w)| {
                        serde_json::json!({
                            "row": row.iter().map(|(n, v)| serde_json::json!({"name": n, "value": v})).collect::<Vec<_>>(),
                            "weight": w,
                        })
                    }).collect::<Vec<_>>(),
                )
            }).collect::<std::collections::HashMap<_, _>>(),
        })
    }
}

/// Lenient ISO-8601 to epoch-milliseconds.
///
/// Handles `YYYY-MM-DDTHH:MM:SS(.fff)?(Z|±HH:MM)?` — the shapes
/// PostgreSQL's text output for timestamptz produces. Not a general
/// date parser: no month names, no timezones beyond the offset.
fn parse_iso_to_ms(s: &str) -> Option<i64> {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() < 19 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let date_part = &s[..10];
    let time_part = &s[11..];
    let year: i64 = date_part[0..4].parse().ok()?;
    let month: i64 = date_part[5..7].parse().ok()?;
    let day: i64 = date_part[8..10].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Split off timezone suffix if present.
    let (hms, tz_offset_min) = if let Some(idx) = time_part.find(['Z', '+']) {
        let (h, tz) = time_part.split_at(idx);
        (h, tz_offset_minutes(tz)?)
    } else {
        (time_part, 0)
    };
    let hour: i64 = hms[0..2].parse().ok()?;
    let minute: i64 = hms[3..5].parse().ok()?;
    let second: i64 = hms[6..8].parse().ok()?;
    let millis: i64 = if hms.len() > 8 && hms.as_bytes()[8] == b'.' {
        let frac = &hms[9..];
        let frac = frac.trim_end_matches(|c: char| !c.is_ascii_digit());
        let digits: i64 = if frac.is_empty() {
            0
        } else {
            let take = frac.len().min(3);
            let mut v: i64 = frac[..take].parse().ok()?;
            for _ in take..3 {
                v *= 10;
            }
            v
        };
        digits
    } else {
        0
    };

    // Days since epoch (civil-from-days algorithm, Howard Hinnant).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;

    let secs = days * 86400 + hour * 3600 + minute * 60 + second - tz_offset_min * 60;
    Some(secs * 1000 + millis)
}

/// Timezone suffix to minutes offset. `Z` = 0; `+HH:MM`, `-HH:MM`,
/// and PostgreSQL's short `+HH` / `-HH` forms.
fn tz_offset_minutes(tz: &str) -> Option<i64> {
    if tz == "Z" {
        return Some(0);
    }
    let sign = match tz.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return None,
    };
    let rest = &tz[1..];
    let (h, m): (i64, i64) = if rest.len() == 5 && rest.as_bytes()[2] == b':' {
        (rest[0..2].parse().ok()?, rest[3..5].parse().ok()?)
    } else if rest.len() == 2 {
        (rest.parse().ok()?, 0)
    } else {
        return None;
    };
    Some(sign * (h * 60 + m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_parse() {
        assert_eq!(parse_iso_to_ms("1970-01-01 00:00:00+00"), Some(0));
        assert_eq!(parse_iso_to_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_iso_to_ms("2024-01-01 00:00:00.500+00"),
            Some(1704067200500)
        );
        assert_eq!(parse_iso_to_ms("not a date"), None);
    }

    #[test]
    fn rows_land_in_correct_windows_and_close() {
        let mut windows = TumblingWindows::new(1000, "ts");
        let mut delta = ZSet::new();
        delta.add(
            vec![("ts".into(), Value::Int(1200)), ("v".into(), Value::Int(5))],
            1,
        );
        delta.add(
            vec![("ts".into(), Value::Int(1700)), ("v".into(), Value::Int(7))],
            1,
        );
        delta.add(
            vec![("ts".into(), Value::Int(2600)), ("v".into(), Value::Int(9))],
            1,
        );
        windows.apply(&delta);

        // Two open windows: [1000, 2000) with 2 rows, [2000, 3000) with 1.
        assert_eq!(windows.windows.len(), 2);
        assert_eq!(windows.windows[&1000].len(), 2);

        // Watermark past 2000 closes the first window.
        let closed = windows.advance_watermark(2100);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].0, 1000);
        assert_eq!(closed[0].1.len(), 2);
        assert_eq!(windows.windows.len(), 1);

        // A late row for the closed window is dropped and counted.
        let mut late = ZSet::new();
        late.add(
            vec![("ts".into(), Value::Int(1500)), ("v".into(), Value::Int(1))],
            1,
        );
        windows.apply(&late);
        assert_eq!(windows.dropped_late, 1);
        // The closed window stays removed (state was reclaimed), and
        // the late row does not reopen it.
        assert!(!windows.windows.contains_key(&1000));
    }
}
