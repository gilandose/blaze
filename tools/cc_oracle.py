#!/usr/bin/env python3
"""Grade blaze's connected components against scipy, on published graphs.

Every correctness test in this repo grades blaze against something blaze's
authors wrote: a BFS reference model, an in-memory compactor, an earlier version
of the same encoder. Those catch regressions well and shared misconceptions not
at all. `scipy.sparse.csgraph.connected_components` has no idea this project
exists, which is the entire point of using it.

WHAT IS CHECKED

For a scope `s`, `scope_root(s, n)` must be the *lowest node id* in `n`'s
connected component of (global edges union edges visible to `s`). Not "a stable
representative" — the exact minimum, which is what makes an answer comparable
across workers and restarts. So this does not compare partitions up to
relabelling; it computes the expected root for every node and checks it value
for value. A partition check would pass an implementation that picked a
different representative on every restart — and that is not hypothetical.
Flipping the union rule from lowest-id-wins to highest-id-wins leaves every
component *identical* and only changes which member represents it; this oracle
reports 4489 of 5086 roots wrong, a partition-equality oracle would report
nothing.

The scope structure is where the interesting bugs would be. Plain connected
components is textbook and the layered per-scope overlay is not, so most of the
work here is building the per-scope oracle: for each scope, the components of
the global subgraph *plus* that scope's own edges.

WHY THE GLOBAL SHARE IS TUNABLE

A real social graph is one giant component, and "every node has the same root"
is a weak thing to verify. Lowering `--global-share` thins the global subgraph
below the percolation threshold, which produces hundreds of real components
whose minima all have to be right. The script prints the component count per
scope so a vacuous run is obvious.

THREE MODES, ONE OF WHICH IS END TO END

`memory` is the in-heap forest and `storage` drives a `Flusher` in-process. Both
are convenient and neither is end to end.

`api` starts the real binary, ingests every edge over HTTP, **restarts the worker
mid-stream**, ingests the rest, and queries the roots back through the API. The
restart is the reason it exists: the second half is answered on top of state the
new process hydrated from the catalog, with a fresh local run cache so the runs
have to come from the object store. Nothing in-process reaches that, and it is
what a real deployment does on every rollout. Breaking hydration turns ~2000 of
5086 roots wrong here and changes nothing in the other two modes.

Usage:
    pip install scipy numpy
    python3 tools/cc_oracle.py                       # vendored graph, all modes
    python3 tools/cc_oracle.py --dataset facebook    # bigger, downloads
    python3 tools/cc_oracle.py --global-share 0.1    # many small components
    python3 tools/cc_oracle.py --modes api           # end to end only
"""

import argparse
import contextlib
import gzip
import http.client
import json
import pathlib
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

ROOT = pathlib.Path(__file__).resolve().parent.parent
CACHE = ROOT / "tools" / ".cache"
VENDORED = ROOT / "tests" / "data"

# Published graphs, fetched from their upstream repositories. Two shapes on
# purpose: a social network that is one giant component, and a word graph that
# is naturally fragmented, so the oracle is exercised at both extremes.
DATASETS = {
    "facebook": {
        "url": "https://raw.githubusercontent.com/benedekrozemberczki/MUSAE/"
        "master/input/edges/facebook_edges.csv",
        "file": "facebook_edges.csv",
        "cite": "Rozemberczki, Allen & Sarkar, Multi-scale Attributed Node "
        "Embedding (2019) — Facebook page-page network",
    },
    "words": {
        "url": "https://raw.githubusercontent.com/networkx/networkx/"
        "main/examples/graph/words_dat.txt.gz",
        "file": "words_dat.txt.gz",
        "cite": "Knuth, The Stanford GraphBase (1993) — five-letter words, "
        "edges between words differing in one position",
    },
}


def fetch(name):
    """Vendored copy if there is one, otherwise download and cache.

    `words` is checked in, so the end-to-end run has no network dependency and
    can gate a merge. `facebook` is 1.9 MB and stays a download — it is the
    bigger, slower run you reach for by hand.
    """
    spec = DATASETS[name]
    vendored = VENDORED / spec["file"]
    if vendored.exists():
        return vendored
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / spec["file"]
    if not path.exists():
        print(f"fetching {spec['url']}")
        urllib.request.urlretrieve(spec["url"], path)
    return path


def load_facebook(path):
    """`id_1,id_2` with a header."""
    raw = np.loadtxt(path, delimiter=",", skiprows=1, dtype=np.int64)
    return raw[:, 0], raw[:, 1]


def load_words(path):
    """Knuth's five-letter words: build the one-letter-apart graph.

    The file lists words, not edges. Two words are adjacent when they differ in
    exactly one position, which is the graph the Stanford GraphBase defines and
    the one every published component count for this dataset refers to.
    """
    words = []
    with gzip.open(path, "rt") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("*"):
                continue
            # The word is the first five characters. What follows is Knuth's
            # frequency annotation and it is not whitespace-separated —
            # "abbot*3,1", "abets+1", "abhor*,,,,19" — so splitting on spaces
            # silently drops most of the file.
            w = line[:5]
            if len(w) == 5 and w.isalpha() and w.islower():
                words.append(w)
    index = {w: i for i, w in enumerate(words)}
    src, dst = [], []
    for w, i in index.items():
        for pos in range(5):
            for c in "abcdefghijklmnopqrstuvwxyz":
                other = w[:pos] + c + w[pos + 1 :]
                j = index.get(other)
                # `j > i` keeps each undirected edge once.
                if j is not None and j > i:
                    src.append(i)
                    dst.append(j)
    # The loader is our code, so it gets graded too: these are the published
    # figures for this graph, and a parser that quietly drops rows would
    # otherwise just make the oracle weaker without making it fail.
    assert len(words) == 5757, f"expected Knuth's 5757 words, parsed {len(words)}"
    assert len(src) == 14135, f"expected 14135 edges, built {len(src)}"
    return np.array(src, dtype=np.int64), np.array(dst, dtype=np.int64)


LOADERS = {"facebook": load_facebook, "words": load_words}


def assign_scopes(n_edges, n_scopes, global_share, seed):
    """Deterministically mark each edge global or visible to exactly one scope.

    One scope per edge, not several: an edge visible to two scopes is just two
    independent merges as far as the oracle is concerned, and modelling it would
    complicate the reference without testing anything the single-scope case does
    not already reach.
    """
    rng = np.random.default_rng(seed)
    scope = rng.integers(1, n_scopes + 1, size=n_edges, dtype=np.int64)
    scope[rng.random(n_edges) < global_share] = 0
    return scope


def expected_roots(src, dst, keep, n_nodes):
    """Lowest node id per component, for the subgraph selected by `keep`.

    scipy hands back arbitrary component labels; the contract is the component
    *minimum*, so labels are turned into minima with one grouped reduction.
    Isolated nodes come out as themselves, which is what blaze returns for a node
    it has never seen.
    """
    s, d = src[keep], dst[keep]
    adj = coo_matrix(
        (np.ones(len(s), dtype=np.int8), (s, d)), shape=(n_nodes, n_nodes)
    )
    n_comp, labels = connected_components(adj, directed=False)
    roots = np.full(n_comp, np.iinfo(np.int64).max, dtype=np.int64)
    np.minimum.at(roots, labels, np.arange(n_nodes, dtype=np.int64))
    return roots[labels], n_comp


# ---------------------------------------------------------------- end to end


def free_port():
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Worker:
    """A real `blaze` process, driven over its HTTP API."""

    def __init__(self, warehouse, data_dir, flush_secs=2):
        self.warehouse, self.data_dir = warehouse, data_dir
        self.flush_secs = flush_secs
        self.proc = None
        self.port = None

    def start(self):
        self.port = free_port()
        self.proc = subprocess.Popen(
            [
                str(ROOT / "target" / "release" / "blaze"),
                "--listen", f"127.0.0.1:{self.port}",
                "--grpc-listen", f"127.0.0.1:{free_port()}",
                "--warehouse", f"file://{self.warehouse}",
                "--table", "graph/edges",
                "--routing-base", "disk",
                "--data-dir", str(self.data_dir),
                "--election", "static:true",
                "--flush-interval-secs", str(self.flush_secs),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 60
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"worker exited with {self.proc.returncode}")
            try:
                if self.get("/v1/health") is not None:
                    return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("worker never became healthy")

    def stop(self):
        if self.proc:
            self.proc.terminate()
            self.proc.wait(timeout=30)
            self.proc = None

    def conn(self):
        return http.client.HTTPConnection("127.0.0.1", self.port, timeout=60)

    def get(self, path, conn=None):
        c = conn or self.conn()
        c.request("GET", path)
        r = c.getresponse()
        body = r.read()
        if conn is None:
            c.close()
        if r.status != 200:
            raise RuntimeError(f"GET {path} -> {r.status}")
        return json.loads(body) if body else {}

    def post_edges(self, edges, workers=8):
        """POST every edge through the real ingest path, keepalive per thread."""

        def send(chunk):
            c = self.conn()
            for src, dst, scope in chunk:
                body = json.dumps(
                    {
                        "src": int(src),
                        "dst": int(dst),
                        # An empty scope list means global; `normalize()` on the
                        # server folds it, and this is the documented shape.
                        "scopes": [] if scope == 0 else [int(scope)],
                    }
                )
                c.request(
                    "POST", "/v1/edges", body,
                    {"Content-Type": "application/json"},
                )
                r = c.getresponse()
                r.read()
                if r.status != 202:
                    raise RuntimeError(f"POST /v1/edges -> {r.status}")
            c.close()

        n = max(1, len(edges) // workers)
        chunks = [edges[i : i + n] for i in range(0, len(edges), n)]
        with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
            list(pool.map(send, chunks))

    def await_ingested(self, count, timeout=180):
        """Wait until the pipeline has consumed `count` events."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            last = self.get("/v1/stats")["last_offset"]
            # `last_offset` is the highest offset assigned, so `count` events
            # means offsets 0..count-1.
            if last + 1 >= count:
                return
            time.sleep(0.2)
        raise RuntimeError(f"pipeline stalled below {count} events")

    def await_committed(self, count, timeout=180):
        """Wait until a snapshot covers `count` events.

        Read from the catalog rather than the API because this is exactly the
        durability boundary: anything above the committed watermark is lost on a
        restart by design, so a restart before this point would be testing the
        RPO, not hydration.
        """
        meta = pathlib.Path(self.warehouse) / "graph" / "edges" / "metadata"
        deadline = time.time() + timeout
        while time.time() < deadline:
            snaps = sorted(meta.glob("snap-*.json")) if meta.exists() else []
            if snaps:
                watermark = json.loads(snaps[-1].read_text())["watermark"]
                if watermark + 1 >= count:
                    return watermark
            time.sleep(0.3)
        raise RuntimeError(f"no snapshot covering {count} events")

    def roots(self, scopes, nodes):
        """Query every (scope, node) through the API."""
        out = {}
        for scope in scopes:
            c = self.conn()
            got = np.empty(len(nodes), dtype=np.int64)
            for i, node in enumerate(nodes):
                got[i] = self.get(
                    f"/v1/scopes/{scope}/components/{int(node)}", conn=c
                )["root"]
            c.close()
            out[scope] = got
        return out


def run_api_mode(edges, scopes, nodes, tmp):
    """Ingest over HTTP, restart the worker mid-stream, then query over HTTP.

    The restart is the reason this is worth doing at all. Everything else here
    is reachable in-process; hydrating a fresh worker from the catalog and having
    it keep answering correctly is not, and it is the step a real deployment
    performs on every rollout.
    """
    warehouse = tmp / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    worker = Worker(warehouse, tmp / "data-a")
    half = len(edges) // 2

    print("  starting worker")
    worker.start()
    print(f"  ingesting {half} edges over HTTP")
    worker.post_edges(edges[:half])
    worker.await_ingested(half)
    watermark = worker.await_committed(half)
    print(f"  committed through offset {watermark}; restarting")
    worker.stop()

    # A different data dir, so the new worker cannot reuse the old one's local
    # run cache and has to pull the runs it needs from the object store.
    worker = Worker(warehouse, tmp / "data-b")
    worker.start()
    print(f"  ingesting the remaining {len(edges) - half} edges")
    worker.post_edges(edges[half:])
    worker.await_ingested(len(edges) - half)
    print(f"  querying {len(scopes)} scopes x {len(nodes)} nodes")
    got = worker.roots(scopes, nodes)
    worker.stop()
    return got


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="facebook", choices=sorted(DATASETS))
    ap.add_argument("--scopes", type=int, default=8)
    ap.add_argument(
        "--global-share",
        type=float,
        default=0.3,
        help="fraction of edges visible to every scope; lower means a sparser "
        "global subgraph and many more components to get right",
    )
    ap.add_argument("--seed", type=int, default=20260801)
    ap.add_argument(
        "--modes",
        default="memory,storage,api",
        help="memory and storage run in-process; api starts the real binary, "
        "ingests over HTTP, restarts it mid-stream and queries over HTTP",
    )
    args = ap.parse_args()

    # Scratch space for generated inputs and per-mode outputs. Created here
    # rather than in `fetch`, which no longer runs at all for a vendored dataset
    # — the CI job found that the hard way on a fresh checkout.
    CACHE.mkdir(parents=True, exist_ok=True)

    spec = DATASETS[args.dataset]
    print(f"dataset: {args.dataset} — {spec['cite']}")
    src, dst = LOADERS[args.dataset](fetch(args.dataset))
    n_nodes = int(max(src.max(), dst.max())) + 1
    print(f"{len(src)} edges, {n_nodes} node ids, {args.scopes} scopes, "
          f"global share {args.global_share}")

    scope = assign_scopes(len(src), args.scopes, args.global_share, args.seed)

    edge_file = CACHE / f"{args.dataset}-edges.txt"
    np.savetxt(edge_file, np.column_stack([src, dst, scope]), fmt="%d")

    # The oracle, per scope: global edges plus that scope's own.
    is_global = scope == 0
    oracle = {}
    for s in [0] + list(range(1, args.scopes + 1)):
        keep = is_global if s == 0 else (is_global | (scope == s))
        roots, n_comp = expected_roots(src, dst, keep, n_nodes)
        oracle[s] = roots
        print(f"  scope {s:<3} {n_comp:>7} components")
    if all(n == 1 for n in [len(np.unique(oracle[0]))]):
        print("  WARNING: the global graph is a single component; this run is weak")

    seen = np.unique(np.concatenate([src, dst]))
    all_scopes = sorted(oracle)
    failures = 0

    for mode in args.modes.split(","):
        print(f"\n=== {mode}")
        if mode == "api":
            subprocess.run(
                ["cargo", "build", "--release", "--quiet", "--bin", "blaze"],
                check=True,
            )
            with tempfile.TemporaryDirectory() as tmp:
                got = run_api_mode(
                    list(zip(src, dst, scope)), all_scopes, seen, pathlib.Path(tmp)
                )
        else:
            out_file = CACHE / f"{args.dataset}-{mode}-roots.txt"
            subprocess.run(
                ["cargo", "run", "--release", "--quiet", "--example", "oracle",
                 "--", str(edge_file), str(out_file), mode, str(args.scopes)],
                check=True,
            )
            rows = np.loadtxt(out_file, dtype=np.int64)
            got = {}
            for s in all_scopes:
                sel = rows[rows[:, 0] == s]
                # blaze is only asked about node ids that appear in the edge
                # list; the oracle's dense array covers every id up to the max.
                assert np.array_equal(np.sort(sel[:, 1]), seen), (
                    f"mode {mode} scope {s}: blaze reported a different node set"
                )
                order = np.argsort(sel[:, 1])
                got[s] = sel[order, 2]

        for s in all_scopes:
            want = oracle[s][seen]
            bad = np.nonzero(got[s] != want)[0]
            if len(bad):
                failures += 1
                print(f"  scope {s}: {len(bad)} of {len(seen)} roots wrong")
                for i in bad[:5]:
                    print(f"    node {seen[i]}: blaze {got[s][i]}, scipy {want[i]}")
            else:
                print(f"  scope {s}: {len(seen)} roots match")

    print()
    if failures:
        print(f"FAILED: {failures} scope/mode combinations disagree with scipy")
        return 1
    print("all scopes in all modes agree with scipy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
