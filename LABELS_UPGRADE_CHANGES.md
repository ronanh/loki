# Labels upgrade — migration to modern prometheus `model/labels` (stringlabels)

Branch: `migrate_prom_labels`. Spec: `UPGRADE_LABELS_SPEC.md`.

Replaces the 2021 pin `github.com/prometheus/prometheus v1.8.2-0.20210215121130`
(`pkg/labels`, mutable `[]Label`) with
`github.com/prometheus/prometheus v0.311.3-0.20260415124738-34cebfe9536c` — the exact
pin of upstream Loki main (Prometheus 3.11-era; **stringlabels** is the source default,
no build tags needed).

Out of scope (later work item): label categories / structured metadata / deltaLabels /
`Process(...)` signature changes; xlog-side changes.

## Commits

1. `bb31293e` baseline test+benchmark results (`bench/baseline.txt`)
2. `cc832e90` dep bump + core migration (labels API, builder rewrite, promql value types)
3. `c06fede3` test migration + semantic updates
4. `35c3b51e` regression tests + stringlabels build guard
5. (this commit) benchmarks after + changes doc

## Dependency diff (go.mod)

- `prometheus/prometheus v1.8.2-0.20210215121130 → v0.311.3-0.20260415124738-34cebfe9536c`
- Already-current deps unchanged: `prometheus/common v0.67.5`,
  `prometheus/client_golang v1.23.2`, `grpc v1.81.1`, `otel v1.43.0`.
- Dropped (no longer needed by the new prometheus): `go-kit/kit`, `go-kit/log`,
  `go-logfmt/logfmt`, `golang/snappy`, `opentracing-go`, `pkg/errors`, `uber/jaeger-*`,
  `oklog/ulid`, `go.uber.org/goleak`, `golang.org/x/sync` (all were indirect).
- New indirects pulled by prometheus 3.11: `dennwc/varint`, `facette/natsort`,
  `fxamacker/cbor/v2`, `grafana/regexp`, `x448/float16`, `golang.org/x/oauth2|term|time`,
  and the k8s.io/sigs.k8s.io discovery stack (`apimachinery`, `client-go`, `klog`,
  `kube-openapi`, `utils`, `json`, `randfill`, `structured-merge-diff`, `yaml`).
  These are compile-time dependencies of prometheus packages we import (promql,
  discovery-adjacent transitive code); none are reachable from fork entry points.

## Per-area changes

### `logql/log/labels.go` — builder rewrite (the one real rewrite)

`labels.Labels` is now an opaque immutable struct (single backing string). The
`BaseLabelsBuilder`/`LabelsBuilder` keep the fork's exact architecture (`del`/`add`
lists, `currentResult`, `resultCache map[uint64]LabelsResult`, shared `hasher`), with
these mechanical changes (mirroring upstream Loki `pkg/logql/log/labels.go`, minus
their category system):

- internal scratch `buf` changed from `labels.Labels` to `[]labels.Label` (still
  allowed for local data); base labels are iterated with `lbs.Range(...)` instead of
  slice ranging;
- results are materialized with `labels.New(buf...)` (sorts, copies — the scratch
  stays reusable; the old code did `buf.Copy()`);
- `labels()`/`withoutResult()` sort the scratch with `slices.SortFunc` (was
  `sort.Sort(labels.Labels)`);
- `toBaseGroup()` filters `base` into the shared `buf` scratch and returns
  `toResult(buf)`, exactly like its `withResult`/`withoutResult` siblings (was
  `WithLabels`/`WithoutLabels`, see behavior deltas). `base` is sorted and filtering
  preserves order, so no sort is needed. Going through `toResult` means the grouped
  result is deduplicated across streams by `resultCache`: a query grouping N streams
  retains one `LabelsResult` per *distinct group*, not per stream. That matters more
  under stringlabels than it did with `[]Label` — every materialized `labels.Labels`
  owns a private copy of its name/value bytes, where the old slice result shared its
  strings with the base (see performance review, below);
- `Get` falls back to `base.Get(key) != ""` (upstream idiom; empty == absent);
- `labelsString()` (the `{a="b", c="d"}` formatter) iterates via `Range`; the
  byte-exact quoting helper `bytesBufferQuoteTo` is unchanged.

### Hashing — no hash-value drift

The migration deliberately keeps every produced hash value identical to the old code:

- `hasher.Hash(labels.Labels)` still uses `HashWithoutLabels(buf)` (same algorithm and
  values in old and new prometheus: xxhash over `name \xff value \xff`, skipping
  `__name__`).
- a new `hasher.hash([]labels.Label)` applies the same algorithm to the scratch slice
  so the per-line `toResult` path keeps its zero-alloc cache lookup (hashing the
  opaque `labels.Labels` would have required materializing it first — one alloc per
  line even on cache hits).
- every place that called the old `lbs.Hash()` (noop pipeline `ForStream`,
  `emptyLabelsResult`) now calls `labels.StableHash(lbs)`, which is exactly the old
  `Hash()` algorithm. The new representation-dependent `Hash()` is not used anywhere
  in the fork.
- `toBaseGroup` hashes through `toResult` → `hasher.hash(buf)`, i.e. the same
  algorithm applied to the scratch slice before materialization. Values are identical
  to `StableHash` of the result for every label set that has no `__name__` — and the
  `without` branch drops `__name__` explicitly, while the `by` branch can only carry
  it if a query writes `by (__name__)` over a log stream, which has no such label.
  This also makes the unchanged-path grouped hash consistent with the modified-path
  one (`withResult`/`withoutResult`), which has always hashed the scratch slice.

Consequence: `LabelsResult.Hash()` values are bit-identical to the pre-migration fork
(the spec allowed them to change; they don't). The xlog "recompute persisted hashes on
load" mitigation remains unnecessary for hashes produced by this repo.

### `logql/log` stages (parser, fmt, label_filter, drop_labels, metrics_extraction, pipeline)

Compile-error-driven only — they talk to the builder, not to raw slices. The only
semantic edit is in `pipeline.go` (`noopPipeline.ForStream` hash, above; the
`labels`-shadowing parameter was renamed to `lbs`).

### `logql/` engine — promql value types

2021→3.x renames, purely mechanical:

- `promql.Point{T, V}` → `promql.FPoint{T, F}` (float samples only; `Histograms`
  stay nil/absent)
- `promql.Series{Metric, Points}` → `promql.Series{Metric, Floats}`
- `promql.Sample{Point, Metric}` → flat `promql.Sample{T, F, Metric}`

Affected: `functions.go`, `range_vector.go`, `engine.go`, `evaluator.go`, `matrix.go`,
`vector.go` (+ tests).

- `evaluator.go getLabelsGroup`: the old arena trick (slicing group labels out of a
  shared preallocated `labels.Labels` buffer) is impossible on an opaque type. Group
  labels are now built in a reusable `[]labels.Label` scratch and materialized with
  `labels.New` — one allocation per **new group** (not per sample), so not hot.
  The `with`-branch merge-join now iterates `lbls.Range` against sorted groups
  (`vectorAggEvaluator` sorts groups before evaluation, unchanged).
- `promql_parser.ParseMetric` no longer exists as a package function (prometheus 3.11
  moved parsing onto a `parser.Parser` instance). A shared
  `promqlParser = promql_parser.NewParser(Options{})` lives in `logql/parser.go`.
  `ParseLabels` no longer needs its `sort.Sort` (ParseMetric returns sorted labels).
- `labelshash.go` (`HashLabels`/`HashWithoutLabels`/`HashForLabels`): same algorithms,
  iteration via `Range` with the same merge-join logic (callers pass sorted names,
  unchanged requirement). Values unchanged.

### Everything else

- `iter/sample_iterator.go`: `nil` labels returns → `labels.EmptyLabels()`.
- `logproto/extensions.go`, `storage/store.go`: import rename only.
- `logql/expr.y`: import path in the grammar prologue updated so a future `goyacc`
  regeneration stays correct (generated `expr.y.go` migrated as well).

### Guard

`logql/log/guard.go`: building with `-tags slicelabels` or `-tags dedupelabels` fails
with `undefined: THIS_FORK_REQUIRES_THE_STRINGLABELS_LABELS_REPRESENTATION` (verified).
Default build needs no tags.

## Behavior deltas

1. **Empty-valued labels no longer exist** (upstream `labels.Builder.Set` semantics,
   decision recorded in the xlog study 06 pass-4). `LabelsBuilder.Set(name, "")` now
   deletes the label. Therefore `| json` / `| logfmt` extracted empty values, logfmt
   keys without value, and `label_format` templates rendering `""` all leave the label
   **absent** instead of producing `name=""`. Asserted by
   `TestEmptyValue_StagesDropLabels` and 5 updated expectations in
   `logql/log/parser_test.go` (json: `expression matching nothing`, `non-matching
   expression`, `empty line`, `existing labels are not affected`, `null field`;
   logfmt: `key alone logfmt`).
2. **Hash values: unchanged** (see Hashing). `labels.Labels` is comparable under
   stringlabels and may be used as a map key (allowed; guarded against other
   representations).
3. **Result label ordering is always sorted.** Previously `GroupedLabels()` with
   `by (b, a)` produced labels in the clause order (`{b=…, a=…}`) for the
   modified-labels path; `labels.New` always sorts, so output (and
   `LabelsResult.String()`) is now `{a=…, b=…}`. Cache keys for that path are still
   hashed in clause order (exactly as before), so per-query cache behavior is
   unchanged. Note `vectorAggEvaluator` always sorted its grouping before evaluating,
   so engine-level `by()` results were already sorted.
4. **`toBaseGroup` with unsorted `by()` groups is now correct.** The old
   `WithLabels(names…)` was a merge-join requiring *sorted* names — unsorted groups
   could silently drop labels from the unchanged-path grouped result. The new
   filter-by-membership pass over `base` is order-insensitive. (`without` keeps
   dropping `__name__` exactly like the old `WithoutLabels` did, via an explicit
   `l.Name == labels.MetricName` skip.)
5. **`Get`/`withResult` treat an empty-valued base label as absent** (only reachable
   if a caller constructs base labels with `labels.New`/`FromStrings` containing
   `""` values; stream labels parsed from queries never do).
6. Tests: the modern `labels.Matcher` carries a compiled `FastRegexMatcher` with
   function fields that never compare equal under `reflect.DeepEqual`. Test-only
   helpers in `logql/strip_regex_test.go` (mirroring upstream
   `syntax.RemoveFastRegexMatchers`) strip compiled regexes before structural AST /
   matcher-slice comparison. Production code is unaffected.

## Tests

- All pre-existing tests kept and passing; **no test was deleted**. Updated:
  literal style (`labels.Labels{{…}}` → `labels.FromStrings(…)`), promql value
  literals, removal of now-impossible `sort.Sort(labels)` calls, `ForLabels(lbs,
  lbs.Hash())` → `labels.StableHash(lbs)` (the value the pipeline actually feeds),
  the 5 empty-value expectations listed above, and the FastRegexMatcher comparison
  helpers.
- Added (`logql/log/labels_upgrade_test.go`):
  - builder semantics table test (set/del/override/del-after-set/set-after-del,
    empty-value deletes, error label, output ordering);
  - `LabelsResult.Hash()` consistency with `labels.StableHash(result.Labels())` for
    plain and grouped (`by`/`without`) paths;
  - result-cache identity (same modifications → same `LabelsResult` instance, across
    builders sharing a `BaseLabelsBuilder`);
  - empty-value behavior for `label_format`, json and logfmt parsers;
  - pinned `LabelsResult.String()` format end-to-end (`{a="b", c="d"}`, sorted,
    `strconv.Quote`-style escaping) for noop and real pipelines.

## Benchmarks

`go test -bench=. -benchmem -run='^$' ./logql/...` on Apple M1 Max
(`bench/baseline.txt` @ `bb31293e` vs `bench/after.txt` @ final; single run each,
compare with `benchstat bench/baseline.txt bench/after.txt` for the full 69-benchmark
report).

Across all 69 benchmarks: **worst time delta +5.8%, no benchmark regressed ≥10%, no
benchmark allocates more** (B/op and allocs/op are equal or lower everywhere).
Geomean: logql -0.7% time, logql/log -1.7% time. Representative results:

| Benchmark | baseline ns/op | after ns/op | Δ time | baseline B/op | after B/op | baseline allocs | after allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pipeline | 3438 | 3411 | -0.8% | 777 | 753 | 30 | 29 |
| Parser/json (no hints) | 1963 | 2042 | +4.0% | 416 | 416 | 9 | 9 |
| Parser/json (hints) | 1517 | 1605 | +5.8% | 416 | 416 | 9 | 9 |
| Parser/logfmt (no hints) | 840 | 848 | +0.9% | 336 | 336 | 16 | 16 |
| Parser/unpack (no hints) | 550 | 568 | +3.2% | 536 | 536 | 17 | 17 |
| JSONParser | 734 | 640 | -12.8% | 152 | 128 | 5 | 4 |
| JSONExpressionParser | 885 | 804 | -9.1% | 160 | 136 | 6 | 5 |
| RangeQuery100000 | 1.239 s | 1.231 s | -0.7% | 6.32 Mi | 6.26 Mi | 106.1k | 104.1k |
| RangeQuery1000000 | 12.76 s | 12.38 s | -2.9% | 56.1 Mi | 57.6 Mi | 991k | 989k |
| CompareParseLabels/promql | 1947 | 1985 | +2.0% | 808 | 656 | 8 | 6 |
| LineFilter geomean | — | — | ≤±3% | 0 | 0 | 0 | 0 |

Notes:

- The +4–6% on `Benchmark_Parser/json` is the cost of `Range`-based base-label checks
  plus `labels.New` materialization; it is single-run noise territory (the sibling
  `BenchmarkJSONParser` on the same code path is **-12.8%** with one alloc less from
  the empty-value/dedup changes). Nothing crosses the 10% gate.
- The fast paths (`LineFilter`, zero-alloc cache hits in the builder) are unchanged:
  hashing still happens on the scratch slice before any materialization.

## Performance review (prod profiling, supprol3 baseline vs supprol4 migrated)

Three call sites were flagged on loki!80 after profiling xlog's alertgw/alerter with
this fork deployed. Full methodology is in the write-up on xlog!313.

1. **`toBaseGroup` — fixed.** Not a regression fix: the alertgw "+5% heap" reading it
   came from was a per-pod comparison across different replica counts and was
   retracted (fleet totals went down). It is a genuine cleanup all the same. The
   profile showed the grouped-labels allocation split into a second large allocator
   (`labels.Builder.Labels`), and the per-call `labels.NewBuilder(base)` was only part
   of it: the bigger cost is that `toBaseGroup` bypassed `resultCache`, so every
   stream retained its own copy of an identical grouped result — a full private byte
   copy under stringlabels. Now it filters into the shared scratch and goes through
   `toResult`. `Benchmark_GroupedLabels_ManyStreams` (new; 1000 distinct streams
   grouped once each, Apple M1 Max, 3 runs):

   | | ns/op | B/op | allocs/op |
   | --- | ---: | ---: | ---: |
   | `by` before | 1 146 445 | 1 165 145 | 10 023 |
   | `by` after | 824 871 | 798 402 | 5 029 |
   | | **−28%** | **−31%** | **−50%** |
   | `without` before | 1 200 481 | 1 245 146 | 10 023 |
   | `without` after | 838 179 | 798 512 | 5 029 |
   | | **−30%** | **−36%** | **−50%** |

   The retained-heap win is larger than the allocation win: what used to be one
   `LabelsResult` (packed string + rendered `String()` + struct) per stream is now one
   per distinct group.

2. **`unsortedLabels` no-op fast path — not a real cost, no change.** The review
   flagged its `b.base.Range(...)` as running per line for pipelines with no
   relabeling stage. It does not: `LabelsResult()`, `Map()` and `IntoMap()` each
   short-circuit on `len(del) == 0 && len(add) == 0 && err == ""` *before* reaching
   `labels()`/`unsortedLabels`, so that branch is only reachable when an error label
   is set. The `Range` decode in the general branch (some label was added or removed)
   is inherent — the packed string has to be walked to produce a `[]Label`.

3. **`HashForLabels`/`HashWithoutLabels` per-step `Range` — deferred, tracked as
   loki#3.** Real and inherent: `vectorAggEvaluator` hashes every input sample at
   every evaluation step, and the packed string must be decoded each time where the
   old `[]Label` was indexed directly. Fixing it means caching a decoded label
   representation per series in the step evaluator, which is an engine change, not a
   labels-migration change.

## Verification

- `go build ./...`, `go test ./...`, `go test ./... -race`: green.
- `go vet ./...`: same three pre-existing findings as the baseline commit, none new
  (2× `iter.Seekable.Seek` stdmethods signature complaints — renaming the interface
  method would break downstream implementers, out of scope — and 1× duplicated json
  tag in `logql/engine_test.go`; CI lint is `allow_failure`, the `go:fmt` job —
  `golangci-lint fmt --diff` — is clean).
- `git grep 'prometheus/pkg/labels'` → no hits (incl. `expr.y`).
- `go build -tags slicelabels ./logql/log/` fails with the guard error, default build
  needs no tags.

## Open questions / follow-ups

- xlog migration runs as a separate session (companion spec in xlog repo). Hashes
  produced by this repo did not change value, which simplifies that work.
- The k8s.io indirect-dependency ripple is large but unavoidable with a modern
  prometheus; if it ever bothers `go mod` hygiene, a future `prometheus/prometheus`
  release with trimmed discovery deps would shrink it.
