//! What is actually in the registry, and what would restructuring it save?
//!
//! The registry maps a shared root to the scopes holding overlay state keyed on
//! it, so a global merge notifies only those scopes instead of broadcasting to
//! thousands. Design 006 measured it at 55% of base bytes and proposed grouping
//! entries by root. That trade is not obviously good: the flat form costs 12
//! bytes per `(root, scope)`, and a grouped form costs `12 + 4k` for a root with
//! `k` scopes — so it *loses* at `k = 1` and only wins from `k = 2`.
//!
//! Which means the saving is entirely decided by the distribution of scopes per
//! root, and that is a property of the graph, not of the format. This measures
//! it rather than assuming it.
//!
//! It also settles the choice design 009 left open: a hand-rolled delta-varint
//! encoding, or Parquet's `DELTA_BINARY_PACKED`, which does the same thing
//! generically and is already a dependency. Both are *written for real* here —
//! the varint column through `storage::RegistryWriter`, the Parquet one through
//! a real writer — because an estimate of a compression scheme is worth very
//! little.
//!
//! Tunables via env: `LINKS`, `NODES`, `SCOPES`, `SCOPES_PER_EDGE`.
//!
//! Run: `cargo run --release --example registry_shape`

use arrow_array::{RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use blaze::core::{EdgeEvent, ScopedForest, Visibility};
use blaze::storage::WriteOptions;
use blaze::storage::{PuffinBase, RegistryEncoding, RegistryWriter, codec, puffin};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;
use std::sync::Arc;

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Write the registry as two sorted integer columns and report the file size.
///
/// `DELTA_BINARY_PACKED` is the encoding that makes this a fair fight: it bit-
/// packs deltas per block of values, so a run of repeated roots costs a
/// fraction of a bit each where a byte-granular varint still spends one whole
/// byte. Dictionary encoding is off — it would win on the scope column for the
/// wrong reason (few distinct scopes) and hide what delta encoding is worth.
fn parquet_bytes(roots: &[u64], scopes: &[u32], compression: Compression) -> u64 {
    let schema = Arc::new(Schema::new(vec![
        Field::new("root", DataType::UInt64, false),
        Field::new("scope", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(roots.to_vec())),
            Arc::new(UInt32Array::from(scopes.to_vec())),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::DELTA_BINARY_PACKED)
        .set_compression(compression)
        // Page statistics are what a lookup would binary-search, so they are
        // part of the format's cost, not an optional extra.
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    out.len() as u64
}

fn main() {
    let links: u64 = env("LINKS", 2_000_000);
    let nodes: u64 = env("NODES", 4_000_000);
    let scopes: u32 = env("SCOPES", 3_000);
    let per_edge: u32 = env("SCOPES_PER_EDGE", 3);

    let forest = ScopedForest::new();
    let mut rng = StdRng::seed_from_u64(0xEE9157);
    for _ in 0..links {
        let visibility = if rng.random_range(0..100u32) < 30 {
            Visibility::Global
        } else {
            Visibility::Scoped(
                (0..1 + rng.random_range(0..per_edge))
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

    // Write the same state under both encodings. The columns below are what the
    // formats *would* cost; these two files are what they do cost, including the
    // blob framing and the in-heap index each reader builds.
    let dir = tempfile::tempdir().unwrap();
    let mut written: Vec<(RegistryEncoding, PuffinBase)> = Vec::new();
    for encoding in [RegistryEncoding::Flat, RegistryEncoding::Blocked] {
        let path = dir.path().join(format!("base-{encoding}.puffin"));
        let blobs = codec::compact_to_blobs(
            &forest,
            1,
            WriteOptions {
                registry: encoding,
                ..Default::default()
            },
        );
        std::fs::write(&path, puffin::write(&blobs, BTreeMap::new())).unwrap();
        written.push((encoding, PuffinBase::open(&path).unwrap()));
    }
    let base = &written[0].1;

    // Registry entries arrive sorted by (root, scope), so a root's scopes are a
    // contiguous run and counting them is a single pass.
    let n = base.registry_len();
    let mut per_root: Vec<u32> = Vec::new();
    let mut roots_col: Vec<u64> = Vec::with_capacity(n);
    let mut scopes_col: Vec<u32> = Vec::with_capacity(n);
    let mut last: Option<u64> = None;
    for (root, scope) in base.registry_iter() {
        if last == Some(root) {
            *per_root.last_mut().unwrap() += 1;
        } else {
            per_root.push(1);
            last = Some(root);
        }
        roots_col.push(root);
        scopes_col.push(scope);
    }

    let roots = per_root.len() as u64;
    let entries = n as u64;
    let flat_bytes = 8 + entries * 12;
    // Grouped: 8-byte header, then per root an 8-byte root, 4-byte count, and a
    // 4-byte scope each.
    let grouped_bytes = 8 + roots * 12 + entries * 4;

    // Delta-varint: entries are sorted, so store the *gap* to the previous root
    // and the gaps between a root's scopes. Roots are nearly dense over the node
    // space, so those gaps are tiny; scope gaps are bounded by the scope count.
    // This is what a columnar format's delta encoding would do automatically —
    // hence the Parquet arm below, which is the same idea bought rather than
    // built.
    let mut writer = RegistryWriter::new(RegistryEncoding::Blocked);
    for (&root, &scope) in roots_col.iter().zip(&scopes_col) {
        writer.push(root, scope);
    }
    let varint_bytes = writer.finish().map(|(_, b)| b.len() as u64).unwrap_or(0);

    let parquet_plain = parquet_bytes(&roots_col, &scopes_col, Compression::UNCOMPRESSED);
    let parquet_zstd = parquet_bytes(
        &roots_col,
        &scopes_col,
        Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
    );

    let mut hist: BTreeMap<&str, u64> = BTreeMap::new();
    for &k in &per_root {
        let bucket = match k {
            1 => "k=1",
            2 => "k=2",
            3..=4 => "k=3-4",
            5..=8 => "k=5-8",
            9..=16 => "k=9-16",
            17..=64 => "k=17-64",
            65..=256 => "k=65-256",
            _ => "k>256",
        };
        *hist.entry(bucket).or_default() += 1;
    }

    let stats = blaze::core::RoutingBase::stats(base);
    let base_bytes = stats.mapped_bytes;
    let pairs = stats.shared_pairs + stats.overlay_pairs;

    println!(
        "{links} links / {nodes} nodes / {scopes} scopes / up to {per_edge} scopes per edge\n"
    );
    println!("pairs                {pairs}");
    println!("registry entries     {entries}");
    println!("distinct roots       {roots}");
    println!(
        "mean scopes/root     {:.2}",
        entries as f64 / roots.max(1) as f64
    );
    println!("\nbase file            {:.1} MB", base_bytes as f64 / 1e6);
    println!(
        "registry (flat)      {:.1} MB   {:.0}% of base",
        flat_bytes as f64 / 1e6,
        100.0 * flat_bytes as f64 / base_bytes as f64
    );
    println!(
        "registry (grouped)   {:.1} MB   {:.0}% of base   -> {:.2}x smaller, base {:.0}% smaller",
        grouped_bytes as f64 / 1e6,
        100.0 * grouped_bytes as f64 / base_bytes as f64,
        flat_bytes as f64 / grouped_bytes as f64,
        100.0 * (flat_bytes - grouped_bytes) as f64 / base_bytes as f64
    );

    let row = |label: &str, bytes: u64| {
        println!(
            "{label:<20} {:.1} MB   {:.0}% of base   -> {:.2}x smaller, base {:.0}% smaller",
            bytes as f64 / 1e6,
            100.0 * bytes as f64 / base_bytes as f64,
            flat_bytes as f64 / bytes as f64,
            100.0 * (flat_bytes as f64 - bytes as f64) / base_bytes as f64
        );
    };
    row("registry (varint)", varint_bytes);
    row("registry (parquet)", parquet_plain);
    row("registry (pq+zstd)", parquet_zstd);

    // Design 011's member index, measured rather than estimated. The estimate in
    // the design (+20-40%, by analogy with what delta-varint did for the
    // registry) assumes an encoding the index does not yet have: it is written
    // as a plain fixed-stride pair table, so it costs what one costs.
    {
        // Both arms at the *same* registry encoding, or the comparison measures
        // the registry rather than the index. `base_bytes` above is the flat
        // arm, which is not the default.
        let mut sizes = [0u64; 2];
        for (i, member_index) in [false, true].into_iter().enumerate() {
            let path = dir
                .path()
                .join(format!("base-members-{member_index}.puffin"));
            let blobs = codec::compact_to_blobs(
                &forest,
                1,
                WriteOptions {
                    member_index,
                    ..Default::default()
                },
            );
            std::fs::write(&path, puffin::write(&blobs, BTreeMap::new())).unwrap();
            sizes[i] = std::fs::metadata(&path).unwrap().len();
        }
        let (without, with) = (sizes[0], sizes[1]);
        println!(
            "\nmember index         +{:.1} MB   run {:.1} -> {:.1} MB   (+{:.0}%, {:.1} B/pair)",
            (with - without) as f64 / 1e6,
            without as f64 / 1e6,
            with as f64 / 1e6,
            100.0 * (with - without) as f64 / without as f64,
            (with - without) as f64 / pairs as f64
        );
    }

    println!("\nscopes per root:");
    for (bucket, count) in &hist {
        let share = 100.0 * *count as f64 / roots.max(1) as f64;
        // Entries, not roots, is what the format cost scales with.
        println!("  {bucket:<9} {count:>10} roots  ({share:.1}%)");
    }

    // A representative miss is a root inside the table's range that simply has
    // no overlay state — not one past the end, which lands in the last block
    // every time and measures a cache-warm full scan rather than a search.
    let present: std::collections::HashSet<u64> = roots_col.iter().copied().collect();
    let absent: Vec<u64> = {
        let mut r = StdRng::seed_from_u64(0xAB5E97);
        (0..50_000)
            .map(|_| r.random_range(0..nodes))
            .filter(|k| !present.contains(k))
            .collect()
    };

    // Block size is the whole latency/heap trade for the blocked encoding: a
    // lookup decodes forward through one block, so halving the block halves the
    // scan and doubles the offsets held in heap. Swept here rather than argued,
    // because the scan is varint decoding and its cost per byte is not something
    // worth predicting.
    println!(
        "\n{:<10} {:>10} {:>11} {:>10} {:>12} {:>12}",
        "block B", "reg MB", "blocks", "heap KB", "hit us", "miss us"
    );
    for block in [256usize, 512, 1024, 2048, 4096, 8192] {
        let mut w = RegistryWriter::with_block_size(RegistryEncoding::Blocked, block);
        for (&root, &scope) in roots_col.iter().zip(&scopes_col) {
            w.push(root, scope);
        }
        let (_, payload) = w.finish().unwrap();
        let reg =
            blaze::storage::registry::BlockedRegistry::parse(&payload, 0..payload.len()).unwrap();
        let blocks = u64::from_le_bytes(payload[8..16].try_into().unwrap());
        let mut qrng = StdRng::seed_from_u64(0x5EA5C);
        let mut timed = |hits: bool| {
            const Q: usize = 20_000;
            let t = std::time::Instant::now();
            let mut sink = 0usize;
            for _ in 0..Q {
                let root = if hits {
                    roots_col[qrng.random_range(0..roots_col.len())]
                } else {
                    absent[qrng.random_range(0..absent.len())]
                };
                sink += reg.scopes_for(&payload, root).len();
            }
            std::hint::black_box(sink);
            t.elapsed().as_secs_f64() * 1e6 / Q as f64
        };
        println!(
            "{block:<10} {:>10.1} {blocks:>11} {:>10.1} {:>12.2} {:>12.2}",
            payload.len() as f64 / 1e6,
            reg.heap_bytes() as f64 / 1024.0,
            timed(true),
            timed(false),
        );
    }

    // What the two encodings actually cost, written and read for real. Index
    // heap is the part that is *not* free to hold — the flat form pays a sparse
    // index, the blocked form an offset and a first root per block — and lookup
    // is the price of the saving, paid only by `apply_global`.
    println!(
        "\n{:<10} {:>10} {:>11} {:>12} {:>12}",
        "encoding", "base MB", "index KB", "hit us", "miss us"
    );
    for (encoding, b) in &written {
        let st = blaze::core::RoutingBase::stats(b);
        // Probe roots that exist and roots that do not, separately: a miss short-
        // circuits as soon as the scan passes the key, so averaging them together
        // would flatter whichever encoding is worse at the one that matters.
        let mut qrng = StdRng::seed_from_u64(0x5EA5C);
        let mut timed = |hits: bool| {
            const Q: usize = 20_000;
            let t = std::time::Instant::now();
            let mut sink = 0usize;
            for _ in 0..Q {
                let root = if hits {
                    roots_col[qrng.random_range(0..roots_col.len())]
                } else {
                    absent[qrng.random_range(0..absent.len())]
                };
                sink += blaze::core::RoutingBase::scopes_for_root(b, root).len();
            }
            std::hint::black_box(sink);
            t.elapsed().as_secs_f64() * 1e6 / Q as f64
        };
        println!(
            "{:<10} {:>10.1} {:>11.1} {:>12.2} {:>12.2}",
            encoding.to_string(),
            st.mapped_bytes as f64 / 1e6,
            st.index_bytes as f64 / 1024.0,
            timed(true),
            timed(false),
        );
    }
}
