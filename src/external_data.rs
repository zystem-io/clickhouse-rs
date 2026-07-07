//! Support for passing [external data] as temporary tables alongside a query.
//!
//! A temporary table is materialized once per request, unlike a `WITH` subquery
//! which the server re-scans for every reference. Attach one with
//! [`Query::with_external_table`][crate::query::Query::with_external_table].
//!
//! [external data]: https://clickhouse.com/docs/engines/table-engines/special/external-data

use bstr::ByteSlice;
use bytes::Bytes;
use std::fmt::Debug;

use crate::error::{Error, Result};

/// The default format assumed by ClickHouse for an external table when no
/// `<name>_format` is provided.
const DEFAULT_FORMAT: &str = "TabSeparated";

/// One external (temporary) table sent with a query.
///
/// The data must already be encoded in the chosen [`format`][Self::with_format].
/// The client does not serialize rows. The `structure` names the columns and
/// their ClickHouse types, e.g. `"id UInt64, name String"`. ClickHouse does
/// not infer it.
///
/// # Example
/// ```
/// # use clickhouse::external_data::ExternalTable;
/// let table = ExternalTable::new("users", "1\tAlice\n2\tBob\n", "id UInt64, name String")?;
/// # let _ = table;
/// # Ok::<_, clickhouse::error::Error>(())
/// ```
#[derive(Clone)]
#[must_use]
pub struct ExternalTable {
    name: String,
    data: Bytes,
    structure: String,
    format: String,
}

impl Debug for ExternalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalTable")
            .field("name", &self.name)
            .field("structure", &self.structure)
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

impl ExternalTable {
    /// Creates a table named `name`, holding pre-formatted `data`, with the
    /// given `structure` (`"col Type, col Type, ..."`).
    ///
    /// The format defaults to `TabSeparated`. Override it with [`Self::with_format`].
    pub fn new(
        name: impl Into<String>,
        data: impl Into<Bytes>,
        structure: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        let data = data.into();
        let structure = structure.into();
        if name.is_empty() {
            return Err(invalid("external table name must not be empty"));
        }
        // The name lands in a `Content-Disposition` header, so it must not carry a
        // quote or newline.
        if name.contains(['"', '\r', '\n']) {
            return Err(invalid(
                "external table name must not contain '\"', CR, or LF",
            ));
        }
        // An empty structure would make ClickHouse reject the request anyway.
        if structure.trim().is_empty() {
            return Err(invalid("external table structure must not be empty"));
        }

        Ok(Self {
            name,
            data,
            structure,
            format: DEFAULT_FORMAT.to_owned(),
        })
    }

    /// Sets the [format] the `data` is encoded in (default `TabSeparated`).
    ///
    /// [format]: https://clickhouse.com/docs/interfaces/formats
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = format.into();
        self
    }

    /// The table name, as referenced in the query.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// The table structure, as a comma-separated list of `col Type` pairs.
    pub(crate) fn structure(&self) -> &str {
        &self.structure
    }

    /// The [format] `data` is encoded in.
    ///
    /// [format]: https://clickhouse.com/docs/interfaces/formats
    pub(crate) fn format_name(&self) -> &str {
        &self.format
    }
}

fn invalid(msg: &'static str) -> Error {
    Error::InvalidParams(msg.into())
}

/// A `multipart/form-data` body as an ordered list of frames plus the boundary
/// chosen for it. Framing bytes are their own small frames. Each table's
/// payload is chained in as its original [`Bytes`].
#[derive(Debug)]
pub(crate) struct Multipart {
    pub(crate) boundary: String,
    pub(crate) frames: Vec<Bytes>,
}

impl Multipart {
    /// Exact body size, for the `Content-Length` header.
    pub(crate) fn content_length(&self) -> usize {
        self.frames.iter().map(Bytes::len).sum()
    }
}

/// Builds the request body for a query carrying external data. The SQL becomes
/// a `query` form field followed by one file part per table.
///
/// `structure` and `format` are not encoded here. They travel as URL query
/// parameters (`<name>_structure`, `<name>_format`). See
/// [`Query::do_execute`][crate::query::Query].
pub(crate) fn build_multipart(query: &str, tables: &[ExternalTable]) -> Multipart {
    let boundary = pick_boundary(query, tables);

    // The query is a form field (no filename). Each table is a file part.
    // ClickHouse's multipart parser only routes parts carrying a `filename`
    // to the external-table handler. The value is ignored, presence is not.
    let mut frames = vec![];
    // The query is small, so its header, body and trailing CRLF fold into one
    // owned frame. The zero-copy path matters only for the table payloads.
    frames.push(Bytes::from(
        part_header(&boundary, crate::settings::QUERY, None) + query + "\r\n",
    ));
    for table in tables {
        frames.push(Bytes::from(part_header(
            &boundary,
            table.name(),
            Some(table.name()),
        )));
        frames.push(table.data.clone()); // shares the buffer, not a copy
        frames.push(Bytes::from_static(b"\r\n"));
    }
    frames.push(Bytes::from(format!("--{boundary}--\r\n")));

    Multipart { boundary, frames }
}

fn part_header(boundary: &str, name: &str, filename: Option<&str>) -> String {
    let mut header = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"");
    if let Some(filename) = filename {
        header.push_str(&format!("; filename=\"{filename}\""));
    }
    header.push_str("\r\n\r\n");
    header
}

/// Picks a boundary that appears in none of the parts, so no payload byte
/// sequence can be mistaken for a delimiter. A distinctive prefix makes the
/// first candidate succeed in practice. The counter guarantees termination.
fn pick_boundary(query: &str, tables: &[ExternalTable]) -> String {
    let occurs = |delim: &str| {
        query.as_bytes().find(delim).is_some()
            || tables.iter().any(|t| t.data.find(delim).is_some())
    };

    (0u64..)
        .map(|n| format!("clickhouse-rs-boundary-{n}"))
        .find(|candidate| !occurs(&format!("--{candidate}")))
        .expect("boundary space is unbounded")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn joined(mp: &Multipart) -> Vec<u8> {
        mp.frames.iter().flatten().copied().collect()
    }

    fn parts(body: &[u8]) -> Vec<&[u8]> {
        body.split_str(b"\r\n").collect()
    }

    #[test]
    fn single_table() {
        let table = ExternalTable::new("ext", "1\tA\n", "id UInt64, name String")
            .unwrap()
            .with_format("TSV");
        let mp = build_multipart("SELECT * FROM ext", std::slice::from_ref(&table));

        let body = joined(&mp);
        let text = body.to_str().unwrap();
        assert!(
            text.contains("name=\"query\""),
            "query field missing: {text}"
        );
        assert!(text.contains("SELECT * FROM ext"), "sql missing: {text}");
        assert!(text.contains("name=\"ext\""), "table field missing: {text}");
        assert!(text.contains("1\tA\n"), "data missing: {text}");
        assert!(
            text.ends_with(&format!("--{}--\r\n", mp.boundary)),
            "missing closing delimiter: {text}"
        );
        assert_eq!(
            mp.content_length(),
            body.len(),
            "Content-Length must equal the concatenated frame bytes"
        );
    }

    #[test]
    fn multiple_tables_each_present() {
        let a = ExternalTable::new("a", "x", "c String").unwrap();
        let b = ExternalTable::new("b", "y", "c String").unwrap();
        let mp = build_multipart("SELECT 1", &[a, b]);

        let count = parts(&joined(&mp))
            .iter()
            .filter(|line| line.starts_with(b"Content-Disposition"))
            .count();
        assert_eq!(count, 3, "expected query + 2 tables, got {count}");
    }

    #[test]
    fn boundary_avoids_collision() {
        // Data literally contains the default boundary delimiter.
        let colliding = "--clickhouse-rs-boundary-0 payload";
        let table = ExternalTable::new("ext", colliding, "c String").unwrap();
        let mp = build_multipart("SELECT 1", std::slice::from_ref(&table));

        assert_ne!(
            mp.boundary, "clickhouse-rs-boundary-0",
            "must have bumped past the colliding boundary"
        );
        // The chosen delimiter appears only as framing, never inside the payload.
        let delim = format!("--{}", mp.boundary);
        assert!(
            !colliding.contains(&delim),
            "payload still contains the chosen boundary"
        );
    }

    #[test]
    fn rejects_name_injection() {
        let err = ExternalTable::new("ev\"il", "x", "c String").unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)), "got {err:?}");
    }

    #[test]
    fn rejects_empty_structure() {
        let err = ExternalTable::new("ext", "x", "   ").unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)), "got {err:?}");
    }

    #[test]
    fn payload_is_not_copied() {
        let data = Bytes::from(vec![7u8; 4096]);
        let original_ptr = data.as_ptr();
        let table = ExternalTable::new("ext", data.clone(), "c String").unwrap();
        let mp = build_multipart("SELECT * FROM ext", std::slice::from_ref(&table));

        let shares_buffer = mp.frames.iter().any(|f| f.as_ptr() == original_ptr);
        assert!(
            shares_buffer,
            "payload was copied. frames should share the original buffer"
        );
    }

    #[test]
    fn frames_preserve_order() {
        let table = ExternalTable::new("ext", "PAYLOAD", "c String").unwrap();
        let mp = build_multipart("SELECT 1", std::slice::from_ref(&table));

        assert_eq!(
            mp.frames.len(),
            5,
            "expected 5 frames, got {}",
            mp.frames.len()
        );
        assert!(
            mp.frames[0].starts_with(b"--"),
            "frame 0 must open with the query part"
        );
        assert!(
            mp.frames[0].ends_with(b"SELECT 1\r\n"),
            "frame 0 must carry the query"
        );
        assert!(
            mp.frames[1].to_str().unwrap().contains("filename=\"ext\""),
            "frame 1 must be the table part header"
        );
        assert_eq!(
            &mp.frames[2][..],
            b"PAYLOAD",
            "frame 2 must be the raw payload alone"
        );
        assert_eq!(
            &mp.frames[3][..],
            b"\r\n",
            "frame 3 must be the trailing CRLF"
        );
        assert_eq!(
            mp.frames[4].to_str().unwrap(),
            format!("--{}--\r\n", mp.boundary),
            "last frame must be the closing delimiter"
        );
    }

    #[test]
    fn content_length_matches_multiple_tables() {
        let a = ExternalTable::new("a", "xxx", "c String").unwrap();
        let b = ExternalTable::new("b", "yyyy", "c String").unwrap();
        let mp = build_multipart("SELECT 1", &[a, b]);

        assert_eq!(
            mp.content_length(),
            joined(&mp).len(),
            "Content-Length must equal the concatenated frame bytes"
        );
    }

    #[test]
    fn empty_payload() {
        let table = ExternalTable::new("ext", "", "c String").unwrap();
        let mp = build_multipart("SELECT 1", std::slice::from_ref(&table));

        assert_eq!(&mp.frames[2][..], b"", "payload frame must be empty");
        assert_eq!(
            mp.content_length(),
            joined(&mp).len(),
            "Content-Length must still equal the frame bytes"
        );
    }

    #[test]
    fn boundary_avoids_collision_in_query() {
        let mut query = "SELECT 'v'".to_owned();
        for n in 0..20 {
            query.push_str(&format!(", '--clickhouse-rs-boundary-{n}'"));
            let table = ExternalTable::new("ext", "x", "c String").unwrap();
            let mp = build_multipart(&query, std::slice::from_ref(&table));

            for i in 0..n {
                assert_ne!(
                    mp.boundary,
                    format!("clickhouse-rs-boundary-{i}"),
                    "must have bumped past the boundary colliding with the query"
                );
            }
            assert!(
                !query.contains(&format!("--{}", mp.boundary)),
                "query still contains the chosen boundary"
            );
        }
    }
}
