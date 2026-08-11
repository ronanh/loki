package log

// Frozen invariants of the upcoming prometheus labels migration (RFC
// supervision-team/rfcs!4, !80). The loki half of what xlog!476 pins on its
// side: the hash xlog persists in StreamDef and in every chunk file, and the
// label strings that reach a query response, must survive the move to
// model/labels (stringlabels) byte-identical.
//
// Snapshots were recorded on PRE-migration code. A diff means STOP the
// migration and flag it — not a re-record. snaps.Update(false) makes a
// repo-wide UPDATE_SNAPS=true a no-op here, and go:test fails on a dirty
// __snapshots__ tree.
//
// The migration forces exactly one edit to this file: the prometheus labels
// import moves from pkg/labels to model/labels. Nothing else here may change.
//
// Deliberately NOT pinned, because !80 documents them as accepted deltas:
// stages producing an empty label value (which becomes a deletion), and
// grouping clauses given in non-sorted order (whose result becomes sorted).
// Fixtures avoid both so that a diff here is always a real regression.

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
)

var frozenSnapshot = snaps.WithConfig(snaps.Update(false))

// processLine and processStage are the only places in this file that name a
// pipeline signature. Keeping the calls here means a future signature change
// is a two-line edit that cannot reach the fixtures or the recorded values.
//
// The timestamp is fixed at 0: these tests are about labels, and no fixture
// uses a stage that reads it.
func processLine(sp StreamPipeline, line string) (string, LabelsResult, bool) {
	return sp.ProcessString(0, line)
}

func processStage(s Stage, line string, lb *LabelsBuilder) ([]byte, bool) {
	return s.Process(0, []byte(line), lb)
}

// labelFixture is input only: every derived value lives in the snapshot.
type labelFixture struct {
	name string
	lbs  labels.Labels
}

var labelFixtures = []labelFixture{
	{
		"typical-k8s",
		labels.FromStrings(
			"cluster", "eu1-c01",
			"datacenter", "eu1",
			"environment", "prod",
			"full_cluster", "eu1-c01.prod",
			"k8s_container_name", "app",
			"k8s_pod_name", "app-7f9c4d-x2x4z",
			"logtype", "applog",
			"path", "/var/log/containers/app_default_app-1234.log",
			"service_id", "svc-123",
		),
	},
	{
		"single-label",
		labels.FromStrings("job", "loki"),
	},
	{
		// quoting is response-visible: LabelsResult.String() must keep escaping
		// exactly as before.
		"special-chars",
		labels.FromStrings(
			"msg", "a\"b\\c\nd",
			"note", "héllo, wörld ✓",
			"zpath", `C:\logs\app.log`,
		),
	},
	{
		// an empty value in the BASE labels (not produced by a stage) stays a
		// present label on both sides; it feeds the hash and the string.
		"empty-value-in-base",
		labels.FromStrings("aempty", "", "bfull", "v"),
	},
	{
		// __name__ is skipped by the builder hash but not by StableHash; the
		// snapshot records both so the divergence stays visible as data.
		"with-metric-name",
		labels.FromStrings("__name__", "up", "instance", "10.0.0.1:9090", "job", "prom"),
	},
}

// TestLabelsResultInvariants records everything derived from a fixture: the
// stream hash, and the hash/string of the LabelsResult on the unchanged path,
// through a real pipeline stage, and through both grouping paths.
func TestLabelsResultInvariants(t *testing.T) {
	for _, lf := range labelFixtures {
		t.Run(lf.name, func(t *testing.T) {
			var b strings.Builder

			fmt.Fprintf(&b, "labels:          %s\n", lf.lbs.String())
			fmt.Fprintf(&b, "builderHash:     %#016x\n", NewBaseLabelsBuilder().Hash(lf.lbs))

			// noop pipeline: this LabelsResult.Hash() is the value xlog
			// persists, and the String() is what a query response shows.
			_, res, ok := processLine(NewNoopPipeline().ForStream(lf.lbs), "a log line")
			require.True(t, ok)
			fmt.Fprintf(&b, "noop.hash:       %#016x\n", res.Hash())
			fmt.Fprintf(&b, "noop.string:     %s\n", res.String())

			// modified path: a stage adds a label, so the result is rebuilt
			// and re-hashed from the builder's working slice.
			f, err := NewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("added", "value")})
			require.NoError(t, err)
			_, res, ok = processLine(NewPipeline([]Stage{f}).ForStream(lf.lbs), "a log line")
			require.True(t, ok)
			fmt.Fprintf(&b, "stage.hash:      %#016x\n", res.Hash())
			fmt.Fprintf(&b, "stage.string:    %s\n", res.String())

			// grouping, sorted clauses only. Both the unchanged path
			// (toBaseGroup) and the modified path (withResult/withoutResult).
			groups := []string{"job", "logtype"}
			for _, without := range []bool{false, true} {
				kind := "by"
				if without {
					kind = "without"
				}

				bb := NewBaseLabelsBuilderWithGrouping(groups, noParserHints, without, false)
				lb := bb.ForLabels(lf.lbs, bb.Hash(lf.lbs))
				lb.Reset()
				gr := lb.GroupedLabels()
				fmt.Fprintf(&b, "%s.unchanged.hash:   %#016x\n", kind, gr.Hash())
				fmt.Fprintf(&b, "%s.unchanged.string: %s\n", kind, gr.String())

				bb = NewBaseLabelsBuilderWithGrouping(groups, noParserHints, without, false)
				lb = bb.ForLabels(lf.lbs, bb.Hash(lf.lbs))
				lb.Reset()
				lb.Set("added", "value")
				gr = lb.GroupedLabels()
				fmt.Fprintf(&b, "%s.modified.hash:    %#016x\n", kind, gr.Hash())
				fmt.Fprintf(&b, "%s.modified.string:  %s\n", kind, gr.String())
			}

			// the map form, sorted for a stable rendering.
			lb := NewBaseLabelsBuilder().ForLabels(lf.lbs, 0)
			lb.Reset()
			m, _ := lb.Map()
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			b.WriteString("map:\n")
			for _, k := range keys {
				fmt.Fprintf(&b, "  %s=%q\n", k, m[k])
			}

			frozenSnapshot.MatchSnapshot(t, b.String())
		})
	}
}

// TestParserStageInvariants pins the labels produced by the three parsers that
// feed extracted labels into the builder, including the duplicate-key suffix
// rule, which is response-visible.
func TestParserStageInvariants(t *testing.T) {
	base := labels.FromStrings("job", "loki", "level", "base")

	for _, tt := range []struct {
		name  string
		stage Stage
		line  string
	}{
		{"json", NewJSONParser(), `{"level":"warn","msg":"hi","nested":{"n":1},"num":3.5,"yes":true}`},
		{"json-duplicate-key", NewJSONParser(), `{"job":"other","level":"warn"}`},
		{"logfmt", NewLogfmtParser(), `level=warn msg="a b" num=3.5 yes=true`},
		{"logfmt-duplicate-key", NewLogfmtParser(), `job=other level=warn`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bb := NewBaseLabelsBuilder()
			lb := bb.ForLabels(base, bb.Hash(base))
			lb.Reset()
			_, ok := processStage(tt.stage, tt.line, lb)
			require.True(t, ok)

			res := lb.LabelsResult()
			var b strings.Builder
			fmt.Fprintf(&b, "line:   %s\n", tt.line)
			fmt.Fprintf(&b, "hash:   %#016x\n", res.Hash())
			fmt.Fprintf(&b, "string: %s\n", res.String())
			frozenSnapshot.MatchSnapshot(t, b.String())
		})
	}
}

// TestRegexpParserInvariants is separate: the constructor can fail.
func TestRegexpParserInvariants(t *testing.T) {
	base := labels.FromStrings("job", "loki")
	p, err := NewRegexpParser(`^(?P<method>\w+) (?P<path>\S+) (?P<status>\d+)$`)
	require.NoError(t, err)

	bb := NewBaseLabelsBuilder()
	lb := bb.ForLabels(base, bb.Hash(base))
	lb.Reset()
	_, ok := processStage(p, "GET /api/v1/query 200", lb)
	require.True(t, ok)

	res := lb.LabelsResult()
	var b strings.Builder
	fmt.Fprintf(&b, "hash:   %#016x\n", res.Hash())
	fmt.Fprintf(&b, "string: %s\n", res.String())
	frozenSnapshot.MatchSnapshot(t, b.String())
}

// Structural invariants. Snapshotting a "true" would tell a reviewer nothing,
// so these stay plain assertions.

// TestUnchangedPathHashIsTheStreamHash is the property xlog depends on: for a
// line no stage touched, the LabelsResult hash is exactly the hash the pipeline
// computed for the stream, which is what gets persisted.
func TestUnchangedPathHashIsTheStreamHash(t *testing.T) {
	for _, lf := range labelFixtures {
		t.Run(lf.name, func(t *testing.T) {
			bb := NewBaseLabelsBuilder()
			streamHash := bb.Hash(lf.lbs)
			lb := bb.ForLabels(lf.lbs, streamHash)
			lb.Reset()
			require.Equal(t, streamHash, lb.LabelsResult().Hash())
		})
	}
}

// TestResultCacheReturnsSameInstance pins the caching contract the builder
// relies on: identical modifications must resolve to one shared LabelsResult,
// including across builders sharing a BaseLabelsBuilder.
func TestResultCacheReturnsSameInstance(t *testing.T) {
	base := labels.FromStrings("job", "loki")
	bb := NewBaseLabelsBuilder()

	lb := bb.ForLabels(base, bb.Hash(base))
	lb.Reset()
	lb.Set("foo", "bar")
	first := lb.LabelsResult()

	lb.Reset()
	lb.Set("foo", "bar")
	require.Same(t, first, lb.LabelsResult(), "same builder, same modification")

	other := bb.ForLabels(base, bb.Hash(base))
	other.Reset()
	other.Set("foo", "bar")
	require.Same(t, first, other.LabelsResult(), "sibling builder, shared cache")
}

// TestLabelsResultSelfConsistency asserts a result's hash is reproducible from
// its own labels — the property that lets a persisted hash be re-derived.
func TestLabelsResultSelfConsistency(t *testing.T) {
	for _, lf := range labelFixtures {
		t.Run(lf.name, func(t *testing.T) {
			bb := NewBaseLabelsBuilder()
			lb := bb.ForLabels(lf.lbs, bb.Hash(lf.lbs))
			lb.Reset()
			lb.Set("added", "value")
			res := lb.LabelsResult()

			require.Equal(t, res.Hash(), NewBaseLabelsBuilder().Hash(res.Labels()))
		})
	}
}

// TestLabelsStringEscapesBackslashAsQuote documents a PRE-EXISTING BUG in
// bytesBufferQuoteTo, found while writing these invariants: the branch handling
// `"` and `\` writes a literal `"` for both, so a backslash in a label value is
// rendered as \" instead of \\. The result does not round-trip through
// ParseLabels and disagrees with prometheus' own labels.Labels.String().
//
// Pinned as-is rather than fixed: these snapshots exist to prove the migration
// changes nothing, and this is response-visible output. Tracked in #5 — fixing
// it is a deliberate, separate re-record.
func TestLabelsStringEscapesBackslashAsQuote(t *testing.T) {
	lbs := labels.FromStrings("path", `C:\logs`)
	res := NewLabelsResult(lbs, 0)

	require.Equal(t, `{path="C:\"logs"}`, res.String(), "current (wrong) rendering")
	require.Equal(t, `{path="C:\\logs"}`, lbs.String(), "what prometheus renders")
}

// TestNoopPipelineCachesPerStream pins that one stream resolves to one
// StreamPipeline instance, so the persisted hash is computed once.
func TestNoopPipelineCachesPerStream(t *testing.T) {
	p := NewNoopPipeline()
	base := labels.FromStrings("job", "loki")
	require.Same(t, p.ForStream(base), p.ForStream(labels.FromStrings("job", "loki")))
}
