# deltaLabels support (WS-B) — changes

Branch: `delta_labels` (stacked on `migrate_prom_labels`). Spec: xlog
`docs/study/line-attrs/` — 03-design-options.md D2.1, 02-upstream-loki.md
(upstream structured-metadata semantics), 04-roadmap-draft.md WS-B.

## API

```go
type StreamPipeline interface {
    BaseLabels() LabelsResult
    Process(ts int64, line []byte, deltaLabels labels.Labels, deltaHash uint64) ([]byte, LabelsResult, bool)
    ProcessString(ts int64, line string, deltaLabels labels.Labels, deltaHash uint64) (string, LabelsResult, bool)
    ReferencedDeltaLabels() bool
}
// StreamSampleExtractor: same additions.

type LabelsResult interface {
    String() string; Labels() labels.Labels; Hash() uint64
    Stream() labels.Labels   // categorized subsets of Labels()
    Delta() labels.Labels
    Parsed() labels.Labels
    Deleted() []string       // stream label names removed by the pipeline
}
```

- `deltaLabels`: per-line additive labels relative to the stream labels
  (decoded from the xlog storage `attrDeltas` column). Additive-only by the
  ingest invariant; empty-valued entries are ignored (frozen format writes
  DEL, never SET "").
- `deltaHash`: stored canonical hash of the line's effective set
  (stream ⊕ delta), `0` = unavailable → recompute. Lines without per-line
  labels pass `(labels.EmptyLabels(), 0)`.

## Semantics (mirrors upstream Loki structured metadata)

- Injection before any stage; even the noop pipeline surfaces delta labels.
- Precedence: **parsed > delta > stream**. Stage `Set`s stay uncategorized
  API-wise (no stage signature changed) and land in the parsed category;
  `Del` hides labels of any category; `__error__` is parsed.
- A delta name colliding with a stream label is renamed `name_extracted`
  at query time and the stored hash is distrusted (forces recompute). The
  ingest collision policy (drop+warn) makes this a foreign/reprocessed-data
  path only. Parsers keep their existing `BaseHas` ⇒ `_extracted` rule
  (stream labels only); a parsed key equal to a delta name simply wins.
- Grouping (`by`/`without`), label filters, `label_format`, `drop` and
  `unwrap` all see delta labels.

## Caching / performance

- Stored-hash fast path: a line whose only label state is its delta (no
  stage touched labels) resolves through
  `resultCache[mix(streamHash, deltaHash)]` and its result reuses
  `deltaHash` as `Hash()` — repeated (stream, delta) combinations do zero
  hashing and zero label materialization (35ns/0 allocs measured).
- Changed paths use a categorized cache key (seeded with the stream hash,
  covering delta/parsed/deleted/error content): equal effective sets with
  different stream/delta/parsed decompositions can never alias to one
  categorized result (regression-tested). `Hash()` values themselves are
  unchanged (same `HashWithoutLabels`-family algorithm as before).
- Grouped results keep the historic content-hash keying (no categorization
  on aggregation outputs).
- `ReferencedDeltaLabels()`: false for the noop pipeline and pipelines made
  of line-only stages (line filters — `StageFunc.lineOnly`, propagated by
  `ReduceStages`), letting the store skip per-line delta decoding and pass
  stored blobs through for `{...} |= "..."`-shaped queries. Unwrap
  extractors: always true; line extractors: false only under `noLabels`.

## Compatibility

- Production code outside `logql/log` has **zero** Process call sites (xlog
  drives the pipelines); the signature change ripples only into this repo's
  tests (all pass `labels.EmptyLabels(), 0`) and into xlog at integration
  time (deferred — xlog stays pinned to the pre-WS-B fork commit until
  then).
- `LabelsResult.Hash()`/`String()` values and the `{a="b", c="d"}` format
  are unchanged; `NewLabelsResult` keeps its signature (whole set counts as
  stream category).
- Hash policy: unchanged — `labels.StableHash`/`HashWithoutLabels` family
  everywhere; the trusted `deltaHash` values are produced xlog-side by
  `xloglabels.Hash` (bit-identical algorithm).

## Benchmarks

`bench/delta-labels/{baseline,after}.txt`
(`go test -bench=. -benchmem -run='^$' ./logql/...`; baseline @ ad0a67b5).
All pre-existing benchmarks within noise (geomean −0.2%), allocations
bit-identical. New delta paths (Apple M1 Max): noop no-delta 2.2ns/0;
stored-hash fast path 35ns/0; recompute 53ns/0; line-filter+delta 59ns/0;
json parser 289→326ns with delta (same allocs); 256-combination
high-cardinality 75ns/0; group-by-delta extractor 47ns/0.

## Commits

1. `delta labels: benchmark baseline (commit zero)`
2. `delta labels: Process(ts, line, deltaLabels, deltaHash) + categorized results`
3. `delta labels: semantics tests + benchmark matrix`
4. after-benchmarks + this doc
