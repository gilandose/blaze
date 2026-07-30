use blaze::core::{EdgeEvent, RoutingBase, ScopedForest, Visibility};
use blaze::storage::{PuffinBase, codec, puffin};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

fn rss_mb() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    let kb: f64 = s
        .lines()
        .find(|l| l.starts_with("VmRSS"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    kb / 1024.0
}

fn main() {
    const EVENTS: usize = 3_000_000;
    const NODES: u64 = 6_000_000;
    const SCOPES: u32 = 3_000;
    let base_rss = rss_mb();

    let mut rng = StdRng::seed_from_u64(9);
    let forest = ScopedForest::new();
    for _ in 0..EVENTS {
        let visibility = if rng.random_range(0..100u8) < 15 {
            Visibility::Global
        } else {
            Visibility::Scoped(smallvec::smallvec![rng.random_range(1..=SCOPES)])
        };
        forest.apply(&EdgeEvent {
            src: rng.random_range(0..NODES),
            dst: rng.random_range(0..NODES),
            visibility,
            event_time_ms: 0,
            props: None,
        });
    }
    let st = forest.stats();
    let links = st.global_links + st.scope_links;
    println!(
        "built: {} links ({} shared, {} scope), RSS {:.0} MB",
        links,
        st.global_links,
        st.scope_links,
        rss_mb() - base_rss
    );

    // Compaction, two ways to the same bytes. Both must pay for the output
    // blobs; only the collecting path also pays for a whole ForestSnapshot
    // first, under the union lock. Measure that intermediate on its own,
    // because it is the term that scales with state.
    let before = rss_mb();
    let t = Instant::now();
    let snap = forest.snapshot();
    let snapshot_ms = t.elapsed().as_secs_f64() * 1000.0;
    let intermediate_mb = rss_mb() - before;
    let pairs = snap.global.len() + snap.scopes.iter().map(|(_, p)| p.len()).sum::<usize>();
    let collect_blobs = codec::snapshot_to_blobs(&snap, 1);
    let collect_ms = snapshot_ms + t.elapsed().as_secs_f64() * 1000.0 - snapshot_ms;
    drop(snap);
    println!(
        "compaction, collecting: {:.0} ms, intermediate ForestSnapshot = {} pairs, +{:.0} MB heap",
        collect_ms, pairs, intermediate_mb
    );

    // Streaming with a sink that counts and drops: compaction's own footprint,
    // with the unavoidable output removed. This is the allocation that goes
    // from ~57 MB here to ~32 GB at 2B links if it is not streamed.
    struct Counting(usize);
    impl blaze::core::SnapshotSink for Counting {
        fn shared_pair(&mut self, _n: u64, _r: u64) {
            self.0 += 1;
        }
        fn overlay_pair(&mut self, _n: u64, _r: u64) {
            self.0 += 1;
        }
    }
    let before = rss_mb();
    let t = Instant::now();
    let mut counting = Counting(0);
    forest.compact_into(&mut counting);
    println!(
        "compaction, streaming:  {:.0} ms, {} pairs emitted, +{:.0} MB heap",
        t.elapsed().as_secs_f64() * 1000.0,
        counting.0,
        rss_mb() - before
    );
    assert_eq!(counting.0, pairs);

    let t = Instant::now();
    let blobs = codec::compact_to_blobs(&forest, 1);
    println!(
        "compaction, streaming into blobs: {:.0} ms",
        t.elapsed().as_secs_f64() * 1000.0
    );
    assert_eq!(
        blobs.iter().map(|b| b.data.len()).sum::<usize>(),
        collect_blobs.iter().map(|b| b.data.len()).sum::<usize>(),
        "the two compaction paths must produce identical payloads"
    );
    drop(collect_blobs);

    let bytes = puffin::write(&blobs, BTreeMap::new());
    let path = std::env::temp_dir().join("blaze-probe-base.puffin");
    std::fs::write(&path, &bytes).unwrap();
    println!(
        "puffin file: {:.0} MB ({} blobs)",
        bytes.len() as f64 / 1e6,
        blobs.len()
    );
    drop(blobs);
    drop(bytes);

    // --- query latency, RAM mode (this forest) ---
    let mut q = StdRng::seed_from_u64(11);
    const Q: usize = 300_000;
    let t = Instant::now();
    let mut sink = 0u64;
    for _ in 0..Q {
        sink += forest.scope_root(q.random_range(0..=SCOPES), q.random_range(0..NODES));
    }
    let ram_q = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    println!(
        "RAM  mode: {:>9.0} lookups/s   ({:.2} us each)",
        Q as f64 / ram_q,
        ram_q * 1e6 / Q as f64
    );
    let ram_rss = rss_mb() - base_rss;
    drop(forest);
    let after_drop = rss_mb();
    println!(
        "after dropping the RAM forest: RSS {:.0} MB",
        after_drop - base_rss
    );

    // Where does open time go?
    let t = Instant::now();
    let f = std::fs::File::open(&path).unwrap();
    let mm = unsafe { memmap2::Mmap::map(&f).unwrap() };
    let map_ms = t.elapsed().as_secs_f64() * 1000.0;
    let t = Instant::now();
    let idx = puffin::read_index(&mm).unwrap();
    let index_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!(
        "open breakdown: mmap {:.1} ms, footer index ({} blobs) {:.1} ms",
        map_ms,
        idx.len(),
        index_ms
    );
    drop(idx);
    drop(mm);

    // --- disk mode: open + serve ---
    let t = Instant::now();
    let mmapped = Arc::new(PuffinBase::open(&path).unwrap());
    let open_ms = t.elapsed().as_secs_f64() * 1000.0;
    let bs = mmapped.stats();
    let disk = ScopedForest::with_base(mmapped);
    println!(
        "disk mode: opened in {:.1} ms ({} shared + {} overlay pairs mapped, {:.0} MB file, \
         {:.1} MB sparse index)",
        open_ms,
        bs.shared_pairs,
        bs.overlay_pairs,
        bs.mapped_bytes as f64 / 1e6,
        bs.index_bytes as f64 / 1e6
    );

    let mut q = StdRng::seed_from_u64(11);
    let t = Instant::now();
    let mut sink = 0u64;
    for _ in 0..Q {
        sink += disk.scope_root(q.random_range(0..=SCOPES), q.random_range(0..NODES));
    }
    let disk_q = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    println!(
        "disk mode: {:>9.0} lookups/s   ({:.2} us each)",
        Q as f64 / disk_q,
        disk_q * 1e6 / Q as f64
    );
    println!(
        "heap+touched pages: RAM mode {:.0} MB vs disk mode {:.0} MB",
        ram_rss,
        rss_mb() - after_drop
    );

    // --- the fold: what keeps heap bounded while the worker runs ---
    // Without it the memtable below would simply keep growing for the life of
    // the process, and the disk tier's RAM argument would expire here.
    let mut w = StdRng::seed_from_u64(31);
    for _ in 0..300_000 {
        disk.apply(&EdgeEvent {
            src: w.random_range(0..NODES),
            dst: w.random_range(0..NODES),
            visibility: Visibility::Scoped(smallvec::smallvec![w.random_range(1..=SCOPES)]),
            event_time_ms: 0,
            props: None,
        });
    }
    let grown = disk.memtable_links();
    let before = rss_mb();
    let fold_path = std::env::temp_dir().join("blaze-probe-fold.puffin");
    let t = Instant::now();
    let mut writer = codec::BlobWriter::new(2);
    let bytes = disk
        .compact_and_fold(&mut writer, |sink| {
            let bytes = puffin::write(&sink.finish(), BTreeMap::new());
            std::fs::write(&fold_path, &bytes)?;
            let base: Arc<dyn blaze::core::RoutingBase> = Arc::new(PuffinBase::open(&fold_path)?);
            anyhow::Ok((base, bytes.len()))
        })
        .unwrap();
    println!(
        "fold: {} memtable links -> 0 in {:.0} ms (ingest stalled; {:.0} MB base rewritten), \
         heap delta {:+.0} MB",
        grown,
        t.elapsed().as_secs_f64() * 1000.0,
        bytes as f64 / 1e6,
        rss_mb() - before
    );
    assert_eq!(disk.memtable_links(), 0);
    assert_eq!(disk.stats().folds, 1);

    let mut q = StdRng::seed_from_u64(11);
    let t = Instant::now();
    let mut sink = 0u64;
    for _ in 0..Q {
        sink += disk.scope_root(q.random_range(0..=SCOPES), q.random_range(0..NODES));
    }
    let post = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    println!(
        "after fold: {:>9.0} lookups/s   ({:.2} us each)",
        Q as f64 / post,
        post * 1e6 / Q as f64
    );

    // --- the same drain, as a delta layer instead of a base rewrite ---
    // Same memtable, same correctness, but the base is appended to rather than
    // rewritten: this is the whole of design 001 in one comparison.
    let mut layered = Arc::new(blaze::storage::LayeredBase::new(Arc::new(
        PuffinBase::open(&path).unwrap(),
    )));
    let delta_forest = ScopedForest::with_base(layered.clone());
    let mut w = StdRng::seed_from_u64(31);
    let mut delta_dir_files = Vec::new();
    for round in 0..3 {
        for _ in 0..300_000 {
            delta_forest.apply(&EdgeEvent {
                src: w.random_range(0..NODES),
                dst: w.random_range(0..NODES),
                visibility: Visibility::Scoped(smallvec::smallvec![w.random_range(1..=SCOPES)]),
                event_time_ms: 0,
                props: None,
            });
        }
        let grown = delta_forest.memtable_links();
        let dpath = std::env::temp_dir().join(format!("blaze-probe-delta-{round}.puffin"));
        let t = Instant::now();
        let mut writer = codec::BlobWriter::new(10 + round);
        let (len, next) = delta_forest
            .fold_delta(&mut writer, |sink, _| {
                let bytes = puffin::write(&sink.finish(), BTreeMap::new());
                std::fs::write(&dpath, &bytes)?;
                let mapped = Arc::new(PuffinBase::open(&dpath)?);
                let next = Arc::new(layered.pushed(mapped));
                anyhow::Ok((
                    next.clone() as Arc<dyn RoutingBase>,
                    (bytes.len(), next.clone()),
                ))
            })
            .unwrap();
        layered = next;
        delta_dir_files.push(dpath);
        println!(
            "delta fold {}: {} links -> 0 in {:>5.0} ms, {:>4.1} MB written ({} layers)",
            round,
            grown,
            t.elapsed().as_secs_f64() * 1000.0,
            len as f64 / 1e6,
            layered.layers()
        );
        assert_eq!(delta_forest.memtable_links(), 0);
    }
    let mut q = StdRng::seed_from_u64(11);
    let t = Instant::now();
    let mut sink = 0u64;
    for _ in 0..Q {
        sink += delta_forest.scope_root(q.random_range(0..=SCOPES), q.random_range(0..NODES));
    }
    let layered_secs = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    println!(
        "base + 3 deltas: {:>9.0} lookups/s   ({:.2} us each)",
        Q as f64 / layered_secs,
        layered_secs * 1e6 / Q as f64
    );

    std::fs::remove_file(&fold_path).ok();
    for f in delta_dir_files {
        std::fs::remove_file(f).ok();
    }
    // RAM-mode cold start for comparison: parse every blob and rebuild the
    // whole forest in the heap.
    let t = Instant::now();
    let raw = std::fs::read(&path).unwrap();
    let blobs = puffin::read(&raw).unwrap();
    let snap = codec::blobs_to_snapshot(&blobs).unwrap();
    let rehydrated = ScopedForest::new();
    rehydrated.hydrate(&snap);
    println!(
        "RAM  mode cold start (read+parse+hydrate): {:.0} ms, RSS {:.0} MB",
        t.elapsed().as_secs_f64() * 1000.0,
        rss_mb() - after_drop
    );
    drop(rehydrated);
    drop(snap);
    drop(blobs);
    drop(raw);

    std::fs::remove_file(&path).ok();
}
