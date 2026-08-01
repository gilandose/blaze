# Vendored graph data

`words_dat.txt.gz` — Knuth's five-letter word list from *The Stanford GraphBase*
(1993), as redistributed by NetworkX under the LGPL. Copied from
`networkx/examples/graph/words_dat.txt.gz` so the end-to-end oracle can run
without network access.

The graph is built by joining words that differ in exactly one position: 5757
words, 14135 edges, 671 of them isolated. `tools/cc_oracle.py` asserts those
figures, so a parser that silently drops rows fails rather than quietly shrinking
the test.
