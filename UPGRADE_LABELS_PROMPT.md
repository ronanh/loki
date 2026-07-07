# Session kickoff prompt — Loki fork labels migration

Copy-paste the block below into a fresh Claude Code session started in `/Users/rhi/PROJECTS/DSCLOUD/code/loki`.

---

Migrate this repo (our Loki fork, `github.com/ronanh/loki`) from the pinned 2021 `prometheus/pkg/labels` to the modern prometheus `model/labels` API with the **stringlabels** representation, aligned with latest upstream Loki.

The full specification is in `UPGRADE_LABELS_SPEC.md` at the repo root — read it first and follow it: target prometheus version, API cheat-sheet, work inventory, semantic changes (empty-valued labels now align with upstream delete-on-empty; Hash() values change and that's accepted), required tests, benchmark baseline/after protocol, and the commit sequence.

Constraints:

- Work only on the current branch `migrate_prom_labels` (verify with `git branch --show-current` before any commit). Never touch `master`.
- First commit = baseline: run the existing tests and benchmarks BEFORE any change and save benchmark output to `bench/baseline.txt` as the spec describes.
- Scope guard: this is a labels-API migration only. Do NOT add label categories, structured metadata, deltaLabels, or change `Process(...)` signatures — that's a separate later work item.
- When unsure how to express something on the new API, mirror upstream Loki main (`pkg/logql/log/labels.go`, `pkg/util/labelpool`) — mechanics only, not their category system.
- Add the regression tests listed in the spec; do not delete failing tests without a justification entry in the changes doc.
- Maintain `LABELS_UPGRADE_CHANGES.md` at the repo root as you go — it is the primary review artifact (per-area changes, behavior deltas, benchmark table baseline vs after, dependency diff, open questions).
- Done = `go build ./... && go vet ./... && go test ./... -race` green, no `pkg/labels` import left, benchmarks compared with no unexplained >10% regression, changes doc complete. Commit in the reviewable steps suggested by the spec.

---
