//! Integration tests for the `0.3.0` minimum-viable implementation.

use pipe_io::sink::{FnSink, NullSink, VecSink};
use pipe_io::source::FnSource;
use pipe_io::{BatchPolicy, ErrorPolicy, Pipeline};

// ---------------------------------------------------------------------
// Smoke tests for the public surface.
// ---------------------------------------------------------------------

#[test]
fn from_iter_map_filter_sink() {
    let sink = VecSink::<i64>::new();
    let handle = sink.handle();
    let stats = Pipeline::from_iter(1..=5)
        .map(|n: i32| i64::from(n) * 10)
        .filter(|n: &i64| *n > 20)
        .sink(sink)
        .run()
        .expect("run");
    assert_eq!(handle.take(), vec![30, 40, 50]);
    assert_eq!(stats.items_in, 5);
}

#[test]
fn filter_map_drops_none() {
    let sink = VecSink::<u32>::new();
    let handle = sink.handle();
    Pipeline::from_iter(["1", "two", "3", "four", "5"])
        .filter_map(|s: &str| s.parse::<u32>().ok())
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(handle.take(), vec![1, 3, 5]);
}

#[test]
fn flat_map_expands_items() {
    let sink = VecSink::<i32>::new();
    let handle = sink.handle();
    Pipeline::from_iter(0..3)
        .flat_map(|n: i32| (0..n).collect::<Vec<_>>())
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(handle.take(), vec![0, 0, 1]);
}

#[test]
fn inspect_observes_without_changing() {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    let total = Arc::new(AtomicI64::new(0));
    let total_for_closure = Arc::clone(&total);

    let sink = VecSink::<i32>::new();
    let handle = sink.handle();
    Pipeline::from_iter(1..=4)
        .inspect(move |n: &i32| {
            total_for_closure.fetch_add(i64::from(*n), Ordering::Relaxed);
        })
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(handle.take(), vec![1, 2, 3, 4]);
    assert_eq!(total.load(Ordering::Relaxed), 10);
}

// ---------------------------------------------------------------------
// Batching
// ---------------------------------------------------------------------

#[test]
fn batch_groups_by_count() {
    let sink = VecSink::<Vec<i32>>::new();
    let handle = sink.handle();
    Pipeline::from_iter(1..=10)
        .batch(BatchPolicy::new().max_items(3))
        .map(|b: pipe_io::Batch<i32>| b.into_inner())
        .sink(sink)
        .run()
        .unwrap();
    let groups = handle.take();
    assert_eq!(groups.len(), 4);
    assert_eq!(groups[0], vec![1, 2, 3]);
    assert_eq!(groups[3], vec![10]);
}

#[test]
fn batch_bytes_groups_by_bytes() {
    let sink = VecSink::<Vec<String>>::new();
    let handle = sink.handle();
    Pipeline::from_iter(["aa", "bb", "cccc", "dd"].iter().map(|s| (*s).to_string()))
        .batch_bytes(BatchPolicy::new().max_bytes(8))
        .map(|b: pipe_io::Batch<String>| b.into_inner())
        .sink(sink)
        .run()
        .unwrap();
    let groups = handle.take();
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].len(), 3);
    assert_eq!(groups[1], vec!["dd".to_string()]);
}

#[test]
#[should_panic(expected = "BatchPolicy must have at least one trigger configured")]
fn batch_panics_on_empty_policy() {
    let _ = Pipeline::from_iter(1..=3)
        .batch(BatchPolicy::new())
        .sink(NullSink::<pipe_io::Batch<i32>>::new());
}

// ---------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------

#[derive(Debug)]
struct ParseFail(&'static str);
impl core::fmt::Display for ParseFail {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "parse fail: {}", self.0)
    }
}

#[test]
fn try_map_fail_fast() {
    let sink = NullSink::<u32>::new();
    let res = Pipeline::from_iter(["1", "x", "3"])
        .stage_id("parse")
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not int")))
        .sink(sink)
        .run();
    let err = res.unwrap_err();
    assert!(matches!(err, pipe_io::Error::Stage { .. }));
    assert_eq!(err.stage().map(|s| s.as_str()), Some("parse"));
}

#[test]
fn try_map_continue_drops_errors() {
    let sink = VecSink::<u32>::new();
    let handle = sink.handle();
    Pipeline::from_iter(["1", "x", "3", "y", "5"])
        .on_error(ErrorPolicy::Continue)
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not int")))
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(handle.take(), vec![1, 3, 5]);
}

#[test]
fn source_errors_are_surfaced() {
    let mut emitted = 0u32;
    let source = FnSource::new(move || -> Result<Option<u32>, &'static str> {
        emitted += 1;
        if emitted == 3 {
            Err("source exploded")
        } else {
            Ok(Some(emitted))
        }
    });
    let res = Pipeline::from_source(source)
        .sink(NullSink::<u32>::new())
        .run();
    let err = res.unwrap_err();
    assert!(matches!(err, pipe_io::Error::Source { .. }));
}

#[test]
fn sink_errors_are_surfaced() {
    let sink = FnSink::new(|_n: u32| -> Result<(), ParseFail> { Err(ParseFail("sink down")) });
    let res = Pipeline::from_iter(0..3).sink(sink).run();
    let err = res.unwrap_err();
    assert!(matches!(err, pipe_io::Error::Sink { .. }));
}

// ---------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------

#[test]
fn sync_driver_runs() {
    let sink = VecSink::<i32>::new();
    let handle = sink.handle();
    let stats = Pipeline::from_iter(0..5).sink(sink).run().unwrap();
    assert_eq!(handle.take(), vec![0, 1, 2, 3, 4]);
    assert_eq!(stats.items_in, 5);
}

#[test]
fn threaded_driver_runs() {
    let sink = VecSink::<i32>::new();
    let handle = sink.handle();
    let stats = Pipeline::from_iter(0..5)
        .map(|n: i32| n * 2)
        .sink(sink)
        .run_threaded()
        .unwrap();
    assert_eq!(handle.take(), vec![0, 2, 4, 6, 8]);
    assert_eq!(stats.items_in, 5);
}

// ---------------------------------------------------------------------
// End-to-end use case: log shipper shape.
// ---------------------------------------------------------------------

#[test]
fn log_shipper_shape() {
    // Sim: take a stream of log lines, parse to (level, body), filter
    // out DEBUG, batch by 4, and emit each batch as a serialized blob.
    let lines = vec![
        "INFO server up",
        "DEBUG poll loop",
        "WARN slow query",
        "ERROR oom",
        "INFO request handled",
        "DEBUG idle",
        "ERROR panic",
        "INFO shutdown",
    ];

    let sink = VecSink::<String>::new();
    let handle = sink.handle();
    Pipeline::from_iter(lines)
        .stage_id("parse")
        .map(|line: &str| {
            let (lvl, body) = line.split_once(' ').unwrap_or(("UNKNOWN", line));
            (lvl.to_string(), body.to_string())
        })
        .stage_id("filter_debug")
        .filter(|(lvl, _): &(String, String)| lvl != "DEBUG")
        .stage_id("batch")
        .batch(BatchPolicy::new().max_items(4))
        .map(|batch: pipe_io::Batch<(String, String)>| {
            let mut s = String::new();
            for (lvl, body) in batch.into_inner() {
                s.push_str(&format!("[{lvl}] {body}\n"));
            }
            s
        })
        .stage_id("ship")
        .sink(sink)
        .run()
        .expect("run");

    let blobs = handle.take();
    assert_eq!(blobs.len(), 2);
    assert!(blobs[0].contains("[INFO] server up"));
    assert!(blobs[0].contains("[WARN] slow query"));
    assert!(!blobs[0].contains("DEBUG"));
    assert!(blobs[1].contains("[INFO] shutdown"));
}

// ---------------------------------------------------------------------
// End-to-end use case: ETL with dead-letter analogue (Continue policy).
// ---------------------------------------------------------------------

#[test]
fn etl_continue_on_enrichment_failure() {
    let rows = vec!["1,alpha", "2,beta", "x,bad", "3,gamma", "y,evil", "4,delta"];

    let sink = VecSink::<(u32, String)>::new();
    let handle = sink.handle();
    Pipeline::from_iter(rows)
        .stage_id("parse")
        .on_error(ErrorPolicy::Continue)
        .try_map(|row: &str| -> Result<(u32, String), ParseFail> {
            let (id_s, name) = row.split_once(',').ok_or(ParseFail("split"))?;
            let id: u32 = id_s.parse().map_err(|_| ParseFail("id"))?;
            Ok((id, name.to_string()))
        })
        .stage_id("sink")
        .sink(sink)
        .run()
        .unwrap();

    let out = handle.take();
    assert_eq!(out.len(), 4);
    assert_eq!(out[0].0, 1);
    assert_eq!(out[3].0, 4);
}
