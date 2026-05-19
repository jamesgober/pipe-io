//! Multi-stage ETL load with `ErrorPolicy::Continue` for resilience.
//!
//! Stages:
//!
//! 1. `parse` - CSV `id,name` parsed to `(u32, String)`. Bad rows
//!    are dropped via `Continue`.
//! 2. `enrich` - looks up a category for each id. Unknown ids fall
//!    back to `"unknown"` rather than failing.
//! 3. `batch` - groups rows into batches of four.
//! 4. `db_writer` - simulates a DB insert by counting batches.
//!
//! Run with:
//!
//! ```text
//! cargo run --example etl
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pipe_io::sink::FnSink;
use pipe_io::{Batch, BatchPolicy, ErrorPolicy, Pipeline};

#[derive(Debug)]
struct ParseError(String);
impl core::fmt::Display for ParseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "parse error: {}", self.0)
    }
}

#[derive(Debug, Clone)]
struct Row {
    id: u32,
    name: String,
    category: &'static str,
}

fn parse_row(line: &str) -> Result<(u32, String), ParseError> {
    let (id_s, name) = line
        .split_once(',')
        .ok_or_else(|| ParseError(format!("expected `,` in `{line}`")))?;
    let id: u32 = id_s
        .parse()
        .map_err(|_| ParseError(format!("bad id `{id_s}`")))?;
    Ok((id, name.to_string()))
}

fn main() {
    let categories: HashMap<u32, &'static str> =
        [(1, "alpha"), (2, "beta"), (3, "gamma"), (5, "epsilon")]
            .into_iter()
            .collect();

    let input: Vec<&str> = vec![
        "1,one",
        "garbage row",
        "2,two",
        "3,three",
        "X,bad-id",
        "4,four",
        "5,five",
        "6,six",
        "7,seven",
        "8,eight",
        "9,nine",
        "10,ten",
    ];

    let rows_loaded = Arc::new(AtomicU64::new(0));
    let batches_loaded = Arc::new(AtomicU64::new(0));

    let rows_for_sink = Arc::clone(&rows_loaded);
    let batches_for_sink = Arc::clone(&batches_loaded);

    let db_writer = FnSink::new(move |batch: Batch<Row>| -> Result<(), &'static str> {
        batches_for_sink.fetch_add(1, Ordering::SeqCst);
        rows_for_sink.fetch_add(batch.len() as u64, Ordering::SeqCst);
        println!("  db write: batch of {} rows", batch.len());
        for row in batch.iter() {
            println!("    [{}] {} ({})", row.id, row.name, row.category);
        }
        Ok(())
    });

    Pipeline::from_iter(input)
        .stage_id("parse")
        .on_error(ErrorPolicy::Continue)
        .try_map(parse_row)
        .stage_id("enrich")
        .map(move |(id, name): (u32, String)| Row {
            id,
            name,
            category: categories.get(&id).copied().unwrap_or("unknown"),
        })
        .stage_id("batch")
        .batch(BatchPolicy::new().max_items(4))
        .stage_id("db_writer")
        .sink(db_writer)
        .run()
        .expect("pipeline run");

    println!(
        "loaded {} rows in {} batches",
        rows_loaded.load(Ordering::SeqCst),
        batches_loaded.load(Ordering::SeqCst)
    );
}
