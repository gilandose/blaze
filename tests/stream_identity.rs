//! Invariant I7, exercised through the real binary.
//!
//! `src/storage/catalog.rs` tests `SnapshotMeta::advances_from`, which is the
//! **commit** path: a snapshot that claims a different stream than its
//! predecessor is rejected. But I7 as stated in `docs/design/README.md` is about
//! **startup** — *"a worker configured for a different stream refuses to start
//! rather than resuming at offsets that mean something else"* — and that check
//! lives in `main`, where nothing reached it.
//!
//! The gap mattered because of what the failure looks like when it is missed.
//! Offsets are only meaningful in the stream that assigned them; a worker
//! pointed at a renamed or rebuilt stream seeks to an offset that exists, gets a
//! record, hydrates cleanly, and serves a table that is wrong in a way nothing
//! downstream can detect. There is no error to notice. So the check has to be
//! the thing that notices, and it had never been run.
//!
//! Both branches are asserted here, the same way `unsafe_commits.rs` does for
//! the preflight gate: refuses by default naming both streams, and starts under
//! `--allow-stream-change` while saying so.

use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A worker over a local-filesystem warehouse, which needs no credentials and
/// keeps this test about identity rather than about object stores.
fn worker(warehouse: &std::path::Path, log: &std::path::Path, extra: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_blaze"));
    cmd.env("RUST_LOG", "info")
        .arg("--warehouse")
        .arg(format!("file://{}", warehouse.display()))
        .arg("--edge-log")
        .arg(log)
        .arg("--edge-log-follow")
        .arg("false")
        .arg("--retention-interval-secs")
        .arg("0")
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

fn ports(base: u16) -> [String; 4] {
    [
        "--listen".into(),
        format!("127.0.0.1:{base}"),
        "--grpc-listen".into(),
        format!("127.0.0.1:{}", base + 1),
    ]
}

fn health(port: u16) -> Option<String> {
    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    sock.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    sock.write_all(b"GET /v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut body = String::new();
    sock.read_to_string(&mut body).ok()?;
    Some(body)
}

fn await_serving(child: &mut Child, port: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(25);
    while Instant::now() < deadline {
        if let Ok(Some(_)) = child.try_wait() {
            return false;
        }
        if let Some(body) = health(port)
            && body.contains("\"status\":\"ok\"")
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

fn collect(out: std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// Stop a worker that is running happily, and read what it said.
fn output(mut child: Child) -> String {
    let _ = child.kill();
    collect(child.wait_with_output().unwrap())
}

/// Wait for a worker that is expected to refuse, and read why.
///
/// Deliberately not `kill`: the refusal is written on the way out, and killing
/// the process first is how the first version of this test managed to assert
/// against an empty string.
fn await_exit(mut child: Child) -> String {
    let deadline = Instant::now() + Duration::from_secs(25);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            assert!(!status.success(), "expected a refusal, got a clean exit");
            return collect(child.wait_with_output().unwrap());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let running = output(child);
    panic!("the worker did not refuse; it was still running after 25s:\n{running}");
}

fn write_log(path: &std::path::Path, n: u64) {
    let mut f = std::fs::File::create(path).unwrap();
    for i in 0..n {
        writeln!(
            f,
            r#"{{"src":{},"dst":{},"visibility":"global","event_time_ms":{}}}"#,
            i % 500,
            (i * 7) % 500,
            1_700_000_000_000u64 + i
        )
        .unwrap();
    }
    f.flush().unwrap();
}

/// Commit a table against one log, then point a worker at another.
///
/// The two logs hold identical records, which is the point: nothing about the
/// *data* is wrong, so only the recorded identity can catch it. That is the case
/// a content check would miss.
#[test]
fn a_worker_pointed_at_a_different_stream_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("wh");
    std::fs::create_dir_all(&warehouse).unwrap();
    let first = dir.path().join("first.ndjson");
    let second = dir.path().join("second.ndjson");
    write_log(&first, 400);
    write_log(&second, 400);

    // Establish the table against `first`, and let it commit a snapshot.
    let mut a = worker(&warehouse, &first, &["--flush-interval-secs", "2"])
        .args(ports(19300))
        .spawn()
        .unwrap();
    assert!(
        await_serving(&mut a, 19300),
        "the first worker never served"
    );
    std::thread::sleep(Duration::from_secs(4));
    let first_log = output(a);
    assert!(
        first_log.contains("committed micro-batch snapshot"),
        "no snapshot was committed, so there is no identity to disagree with:\n{first_log}"
    );

    // Same data, different stream. Must refuse.
    let second_worker = worker(&warehouse, &second, &[])
        .args(ports(19310))
        .spawn()
        .unwrap();
    let refused = await_exit(second_worker);
    assert!(
        refused.contains("first.ndjson") && refused.contains("second.ndjson"),
        "the refusal must name both streams so an operator can see which is which:\n{refused}"
    );
    assert!(
        refused.contains("--allow-stream-change"),
        "the refusal must name the way out:\n{refused}"
    );
}

/// The override, which exists for a stream renamed or moved with its offsets
/// intact — a topic migration. It is the branch an operator reaches while
/// already mid-incident, so a slip in it (a flag that quietly does nothing, a
/// panic in the warning loop) would only ever surface then.
#[test]
fn the_override_starts_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("wh");
    std::fs::create_dir_all(&warehouse).unwrap();
    let first = dir.path().join("first.ndjson");
    let renamed = dir.path().join("renamed.ndjson");
    write_log(&first, 400);
    write_log(&renamed, 400);

    let mut a = worker(&warehouse, &first, &["--flush-interval-secs", "2"])
        .args(ports(19320))
        .spawn()
        .unwrap();
    assert!(
        await_serving(&mut a, 19320),
        "the first worker never served"
    );
    std::thread::sleep(Duration::from_secs(4));
    assert!(output(a).contains("committed micro-batch snapshot"));

    let mut b = worker(&warehouse, &renamed, &["--allow-stream-change"])
        .args(ports(19330))
        .spawn()
        .unwrap();
    let served = await_serving(&mut b, 19330);
    let log = output(b);
    assert!(
        served,
        "--allow-stream-change did not let the worker start:\n{log}"
    );
    assert!(
        log.contains("allow-stream-change") || log.contains("changed stream"),
        "the override must be loud about what it allowed:\n{log}"
    );
}
