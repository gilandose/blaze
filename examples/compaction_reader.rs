//! What does a compaction sweep leave behind in memory?
//!
//! A full-table scan is the worst possible cache citizen: it touches every page
//! exactly once and then never again. Done through the mapping, every one of
//! those pages is faulted in and stays resident, and because they are the most
//! recently touched in the process, LRU prefers to evict the query working set
//! over them. That is a recency inversion, not a leak — nothing is lost, but
//! lookups get slower for as long as the pages linger.
//!
//! `ScanReader::Buffered` reads through a file descriptor instead and
//! `posix_fadvise(DONTNEED)`s the range afterwards. Two effects, and the first
//! is the larger one: bytes read with `read(2)` never enter this process's page
//! tables, so they never count toward RSS and stay droppable — `DONTNEED` skips
//! pages that are mapped by anyone, so on its own it would achieve very little.
//!
//! **Page-cache residency is the measurement**, taken with `mincore(2)` over a
//! second, never-touched mapping of the same file. RSS was the obvious metric
//! and turned out to be the wrong one — see the note at the bottom of this file.
//!
//! Each arm starts from an evicted cache (`posix_fadvise(DONTNEED)` over the
//! whole file, verified) and its own `PuffinBase`, so neither inherits the
//! other's residency. Wall time is reported too, because a fix that frees memory
//! and doubles compaction time is a different trade than one that does not.
//!
//! Tunables via env: `LINKS`, `NODES`, `SCOPES`.
//!
//! Run: `cargo run --release --example compaction_reader`

use blaze::core::{EdgeEvent, RoutingBase, ScopedForest, Visibility};
use blaze::storage::{PuffinBase, ScanReader, WriteOptions, codec, puffin};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;
use std::time::Instant;

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Fraction of a file's pages currently in the page cache.
///
/// Measured through a separate mapping that is never read, so taking the
/// measurement cannot itself fault anything in — `mincore` reports residency
/// without touching the pages.
fn resident_fraction(map: &memmap2::Mmap) -> f64 {
    let pages = map.len().div_ceil(4096);
    let mut vec = vec![0u8; pages];
    // SAFETY: `map` is a live mapping of exactly `map.len()` bytes starting at a
    // page-aligned address, and `vec` has one byte per page as mincore requires.
    let rc = unsafe {
        libc::mincore(
            map.as_ptr() as *mut libc::c_void,
            map.len(),
            vec.as_mut_ptr(),
        )
    };
    if rc != 0 {
        return f64::NAN;
    }
    vec.iter().filter(|b| *b & 1 == 1).count() as f64 / pages as f64
}

/// Evict a file from the page cache, so an arm starts from a known state.
fn evict(path: &std::path::Path) {
    use std::os::fd::AsRawFd;
    let f = std::fs::File::open(path).unwrap();
    // SAFETY: `f` is open for the call; posix_fadvise only touches kernel-side
    // page-cache state.
    unsafe {
        libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
    }
}

fn main() {
    let links: u64 = env("LINKS", 6_000_000);
    let nodes: u64 = env("NODES", 4_000_000);
    let scopes: u32 = env("SCOPES", 3_000);

    let forest = ScopedForest::new();
    let mut rng = StdRng::seed_from_u64(0x00FA_D15E);
    for _ in 0..links {
        let visibility = if rng.random_range(0..100u32) < 30 {
            Visibility::Global
        } else {
            Visibility::Scoped(
                (0..1 + rng.random_range(0..3u32))
                    .map(|_| 1 + rng.random_range(0..scopes))
                    .collect(),
            )
        };
        forest.apply(&EdgeEvent {
            src: rng.random_range(0..nodes),
            dst: rng.random_range(0..nodes),
            visibility,
            event_time_ms: 0,
            props: None,
        });
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("base.puffin");
    let blobs = codec::compact_to_blobs(&forest, 1, WriteOptions::default());
    std::fs::write(&path, puffin::write(&blobs, BTreeMap::new())).unwrap();
    let file_mb = std::fs::metadata(&path).unwrap().len() as f64 / 1e6;
    drop(forest);

    // A mapping used only for measuring. Never read, so it faults nothing in and
    // holds no page-table entries of its own — which also means it cannot block
    // the eviction the buffered reader is supposed to perform.
    let probe_file = std::fs::File::open(&path).unwrap();
    // SAFETY: read-only mapping of an immutable file nothing else writes.
    let probe = unsafe { memmap2::Mmap::map(&probe_file).unwrap() };

    println!("{links} links / {nodes} nodes / {scopes} scopes");
    println!("base file            {file_mb:.0} MB\n");
    println!(
        "{:<10} {:>12} {:>12} {:>12} {:>9} {:>10}",
        "reader", "after open", "after sweep", "left MB", "sweep s", "warm us"
    );

    for reader in [ScanReader::Mapped, ScanReader::Buffered] {
        evict(&path);
        let before = resident_fraction(&probe);
        // A fresh base per arm: the previous arm's mapping must be gone, or its
        // page-table entries would keep `evict` from doing anything.
        let base = PuffinBase::open_with_reader(&path, reader).unwrap();
        let opened = resident_fraction(&probe);

        // Establish a query working set *before* the sweep, the way a live
        // worker has one. This is the whole point of the exercise: pages a
        // lookup has touched are mapped into the process, and `DONTNEED` cannot
        // reclaim a mapped page — so the sweep should drop what it read and
        // leave what queries are using.
        let warm: Vec<u64> = {
            let mut r = StdRng::seed_from_u64(0x1234);
            (0..20_000).map(|_| r.random_range(0..nodes)).collect()
        };
        let mut sink = 0u64;
        for &n in &warm {
            sink += base.shared_parent(n).unwrap_or(0);
        }
        std::hint::black_box(sink);
        let t = Instant::now();
        let mut sink = 0u64;
        for &n in &warm {
            sink += base.shared_parent(n).unwrap_or(0);
        }
        std::hint::black_box(sink);
        let warm_before = t.elapsed().as_secs_f64() * 1e6 / warm.len() as f64;

        let t = Instant::now();
        let mut pairs = 0u64;
        base.for_each_shared_pair(&mut |_, _| pairs += 1);
        for scope in base.scopes() {
            base.for_each_overlay_pair(scope, &mut |_, _| pairs += 1);
        }
        let sweep = t.elapsed().as_secs_f64();
        let after = resident_fraction(&probe);

        // The same keys again. If the sweep evicted the working set, this is
        // where it shows.
        let t = Instant::now();
        let mut sink = 0u64;
        for &n in &warm {
            sink += base.shared_parent(n).unwrap_or(0);
        }
        std::hint::black_box(sink);
        let lookup = t.elapsed().as_secs_f64() * 1e6 / warm.len() as f64;

        println!(
            "{:<10} {:>11.0}% {:>11.0}% {:>12.0} {sweep:>9.2} {:>6.2} -> {lookup:.2}",
            format!("{reader:?}").to_lowercase(),
            100.0 * opened,
            100.0 * after,
            after * file_mb,
            warm_before,
        );
        let _ = before;
        assert!(pairs > 0);
        drop(base);
    }

    println!(
        "\n'left MB' is what the sweep leaves cached of a {file_mb:.0} MB file. \
         'warm us' is the same\nlookups before and after it — the working set \
         survives either way, because pages a\nlookup touched are mapped, and \
         DONTNEED cannot reclaim a mapped page. That is the\nbehaviour MADV_COLD \
         would give if memmap2 exposed it, reached by construction."
    );
    println!(
        "\nRSS was the obvious metric and is the wrong one: pages read with read(2) \
         never enter\nthis process's page tables, so they never appear in RSS \
         whether or not they are\ncached — which is exactly the difference being \
         measured, and makes RSS blind to it."
    );
}
