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

Usage:
    pip install scipy numpy
    python3 tools/cc_oracle.py                     # default dataset, both modes
    python3 tools/cc_oracle.py --dataset words     # a different graph shape
    python3 tools/cc_oracle.py --global-share 0.1  # many small components
"""

import argparse
import gzip
import pathlib
import subprocess
import sys
import urllib.request

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

CACHE = pathlib.Path(__file__).resolve().parent / ".cache"

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
    spec = DATASETS[name]
    CACHE.mkdir(exist_ok=True)
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
        "--modes", default="memory,storage", help="comma-separated blaze modes"
    )
    args = ap.parse_args()

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

    failures = 0
    for mode in args.modes.split(","):
        out_file = CACHE / f"{args.dataset}-{mode}-roots.txt"
        cmd = [
            "cargo", "run", "--release", "--quiet", "--example", "oracle",
            "--", str(edge_file), str(out_file), mode, str(args.scopes),
        ]
        print(f"\n=== {mode}")
        subprocess.run(cmd, check=True)

        got = np.loadtxt(out_file, dtype=np.int64)
        seen = np.unique(np.concatenate([src, dst]))
        for s in sorted(oracle):
            rows = got[got[:, 0] == s]
            # blaze is only asked about node ids that appear in the edge list;
            # the oracle's dense array covers every id up to the maximum.
            nodes, roots = rows[:, 1], rows[:, 2]
            assert np.array_equal(np.sort(nodes), seen), (
                f"mode {mode} scope {s}: blaze reported a different node set"
            )
            want = oracle[s][nodes]
            bad = np.nonzero(roots != want)[0]
            if len(bad):
                failures += 1
                print(f"  scope {s}: {len(bad)} of {len(nodes)} roots wrong")
                for i in bad[:5]:
                    print(f"    node {nodes[i]}: blaze {roots[i]}, scipy {want[i]}")
            else:
                print(f"  scope {s}: {len(nodes)} roots match")

    print()
    if failures:
        print(f"FAILED: {failures} scope/mode combinations disagree with scipy")
        return 1
    print("all scopes in all modes agree with scipy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
