# Spec — Migrate fork to modern prometheus labels (stringlabels)

Branch: `migrate_prom_labels`. Scope: this repo (`github.com/ronanh/loki`) only; the companion xlog migration runs afterwards in a separate session (spec lives in xlog: `docs/study/line-attrs/07-upgrade-labels-xlog-spec.md`).

## Goal

Replace `github.com/prometheus/prometheus/pkg/labels` (pinned 2021, mutable `[]Label`) with the modern opaque labels API (`model/labels`), targeting the **stringlabels** representation as used by latest upstream Loki. Behavior-preserving except the documented semantic changes below. **Out of scope**: label categories / structured-metadata / deltaLabels / `Process` signature changes (that is a later work item — do not pre-build it here), and any xlog-side change.

## Target versions

- `github.com/prometheus/prometheus v0.311.3-0.20260415124738-34cebfe9536c` — the exact pin of upstream Loki main (Prometheus 3.11-era; **stringlabels is the source default since 3.5**, so no build tags are needed). If `go mod tidy` fights this pseudo-version, any v0.305.0+ release tag is acceptable — but prefer matching upstream Loki and record the final choice in the changes doc.
- Expect a dependency ripple (`prometheus/common`, otel, grpc, etc.); accept what `go mod tidy` requires, keep the diff reviewable.

## Reference implementation

When unsure how to express something on the new API, **look at how upstream Loki main does it** — it completed this exact migration (issue grafana/loki#17122, final PR grafana/loki#18490):

- `https://github.com/grafana/loki/blob/main/pkg/logql/log/labels.go` — builder on the new API (note: upstream has label *categories*; do NOT port categories, only the API mechanics)
- `https://github.com/grafana/loki/tree/main/pkg/util/labelpool` — pooled `ScratchBuilder` pattern for hot paths
- `https://github.com/prometheus/prometheus/blob/main/model/labels/labels_stringlabels.go` — the representation itself

## API migration cheat-sheet

| Old (pkg/labels, 2021) | New (model/labels) |
| --- | --- |
| import `.../pkg/labels` | import `.../model/labels` |
| `lbs[i]`, `for _, l := range lbs` | `lbs.Range(func(l labels.Label) {...})` |
| `append(lbs, labels.Label{...})`, `make(labels.Labels, n)` | `labels.ScratchBuilder` (Add → Sort → Labels), or `labels.Builder` to edit an existing set |
| `sort.Sort(lbs)` | `ScratchBuilder.Sort()` before `Labels()` |
| `lbs[i].Value = v` (in-place) | `labels.NewBuilder(lbs).Set(n, v).Labels()` |
| `len(lbs)` | `lbs.Len()` (documented slow under stringlabels — avoid in loops) |
| `labels.Labels{}` / `nil` | `labels.EmptyLabels()` (and `lbs.IsEmpty()`) |
| `labels.Labels{{...}}` literals (tests) | `labels.FromStrings("n1","v1","n2","v2")` |
| `lbs.Copy()` for safety | usually unnecessary (immutable); keep only where lifetime decoupling from unsafe buffers is the point |

Semantics changes to embrace (and test):

1. **`Builder.Set(name, "")` deletes the label.** Empty-valued labels no longer exist. Fork code/tests that produce or assert empty-valued labels must be updated to the upstream behavior (decision recorded in xlog study, 06 pass-4).
2. **`Hash()` values change** (stringlabels hashes the encoded backing string). In-process consistency is guaranteed; any test with hardcoded hash values must be updated. Cross-process/persisted-hash concerns are handled on the xlog side (recompute on load), not here.
3. `labels.Labels` is comparable under stringlabels (usable as map key, `==`). Allowed — we do not support `slicelabels`/`dedupelabels` builds (see Guard below).

## Work inventory (verified counts on this branch)

29 files import `pkg/labels`; 12 import `promql`; 6 import `promql/parser`; 1 imports `util/strutil`.

### A. `logql/log/` — the careful part (13 label files)

- `labels.go` — **the one real rewrite**. `BaseLabelsBuilder`/`LabelsBuilder` currently mutate `add []labels.Label` / `buf labels.Labels` in place, `sort.Sort(b.buf)`, build via append (`labels.go:320-379`, `:443-460`). Re-implement on the new API keeping the fork's exact semantics (del/add lists, `currentResult`, `resultCache map[uint64]LabelsResult`, `LabelsResult{String,Labels,Hash}`). Internal scratch state may keep `[]labels.Label` slices (that is still allowed for *local* data) — only the public `labels.Labels` values must follow the new API. Use upstream main's `labels.go` as the mechanical reference, stripped of categories.
- `pipeline.go`, `metrics_extraction.go`, `parser.go`, `fmt.go`, `label_filter.go`, `drop_labels.go`, `labels_filter_duration.go` etc. — mostly compile-error-driven: they talk to the builder, not to raw slices. Watch `fmt.go` label templates and `parser.go` extracted-label paths for empty-value behavior.
- Add a pooled-ScratchBuilder helper (mirror upstream `labelpool`) if per-line building shows up in benchmarks.

### B. `logql/` engine — promql surface (12 files)

Value-type renames from the 2021 promql to 3.x:

- `promql.Point{T, V}` → `promql.FPoint{T, F}` (histogram twin `HPoint` exists; the fork produces float samples only — ignore `Histograms` fields, leave them nil/empty)
- `promql.Series{Metric, Points}` → `promql.Series{Metric, Floats []FPoint, Histograms []HPoint}`
- `promql.Sample{Point, Metric}` → `promql.Sample{T, F, H, Metric}`
- `promql.Vector`/`Matrix` shapes follow from the above; check `String()`/sort helpers used in tests.
- `promql/parser` and `util/strutil`: minor; fix on compile errors.

### C. Everything else

`iter/` (1), `logproto/` (1), `storage/` (1), `pattern/` if it touches labels — mechanical.

### D. Guard

Add a small `guard.go` (e.g. in `logql/log/`):

```go
//go:build slicelabels || dedupelabels

package log

// This fork requires the stringlabels representation (comparable labels.Labels,
// zero-alloc Hash). Building with slicelabels/dedupelabels is unsupported.
var _ = func() { panic("unsupported labels representation; build without slicelabels/dedupelabels") }
```

(or a compile-time trick of your choice — the point is failing fast on wrong build tags).

## Process requirements

1. **Baseline first** (separate commit before any change): run `go test ./...` and the existing benchmarks (`go test -bench=. -benchmem -run=^$ ./logql/... ` at minimum), save outputs to `bench/baseline.txt` (create dir). The same benchmarks re-run at the end go to `bench/after.txt`.
2. Commit in reviewable steps (suggested: ① go.mod bump + import renames + mechanical fixes until it compiles; ② labels.go builder rewrite; ③ promql value types; ④ tests/semantic updates; ⑤ benchmarks + polish). Branch only; never touch `master`.
3. `go build ./... && go vet ./... && go test ./... -race` green at the end (and per-commit where feasible).
4. **Tests to add** (regression intent):
   - builder semantics table-test: set/del/override on base labels, ordering of output, `LabelsResult.Hash()` self-consistency, result-cache hit behavior;
   - empty-value behavior: `label_format`/parsers producing `""` → label absent (the new upstream-aligned behavior, asserted explicitly);
   - a pipeline end-to-end test pinning `LabelsResult.String()` output format (it is response-visible downstream — `{a="b", c="d"}` formatting must not drift);
   - keep/adapt every existing test — deleting a failing test requires a justification line in the changes doc.
5. **Changes doc** (the main review artifact): write `LABELS_UPGRADE_CHANGES.md` at repo root as you go — one section per area: what changed, why, behavior deltas (esp. empty-values, hash values, any test rewritten), benchmark table (baseline vs after: ns/op, B/op, allocs/op), dependency diff summary, open questions.

## Acceptance

- All tests green with `-race`; no remaining `pkg/labels` import; no build tags required to build.
- Benchmarks: no unexplained regression >10% on the logql/log hot paths (explain or fix anything beyond that in the changes doc).
- `LABELS_UPGRADE_CHANGES.md` complete enough for a reviewer who hasn't followed the work.
