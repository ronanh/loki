package log

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

var (
	dlBase = labels.FromStrings("app", "nginx", "cluster", "us-central1")
	// typical OTel-shaped per-line delta
	dlDelta = labels.FromStrings(
		"severity_text", "ERROR",
		"trace_id", "0af7651916cd43dd8448eb211c80319c",
	)
)

func dlHash(base labels.Labels, delta labels.Labels, extra ...string) uint64 {
	b := labels.NewBuilder(base)
	delta.Range(func(l labels.Label) { b.Set(l.Name, l.Value) })
	for i := 0; i < len(extra); i += 2 {
		b.Set(extra[i], extra[i+1])
	}
	return labels.StableHash(b.Labels())
}

func TestNoopPipelineDeltaLabels(t *testing.T) {
	p := NewNoopPipeline()
	sp := p.ForStream(dlBase)

	// empty delta: constant per-stream result, identical to the historic path
	_, res, ok := sp.Process(0, []byte("line"), labels.EmptyLabels(), 0)
	require.True(t, ok)
	require.Equal(t, dlBase, res.Labels())
	require.Equal(t, labels.StableHash(dlBase), res.Hash())
	require.False(t, sp.ReferencedDeltaLabels())
	require.Equal(t, dlBase, sp.BaseLabels().Labels())

	// delta-carrying line: labels merged, categorized
	storedHash := dlHash(dlBase, dlDelta)
	_, res, ok = sp.Process(0, []byte("line"), dlDelta, storedHash)
	require.True(t, ok)
	wantAll := labels.FromStrings(
		"app", "nginx", "cluster", "us-central1",
		"severity_text", "ERROR", "trace_id", "0af7651916cd43dd8448eb211c80319c",
	)
	require.Equal(t, wantAll, res.Labels())
	require.Equal(t, storedHash, res.Hash())
	require.Equal(t, dlBase, res.Stream())
	require.Equal(t, dlDelta, res.Delta())
	require.True(t, res.Parsed().IsEmpty())
	require.Empty(t, res.Deleted())

	// repeated combination: cached identity (zero rebuild)
	_, res2, _ := sp.Process(0, []byte("other line"), dlDelta, storedHash)
	require.Same(t, res, res2)
}

func TestDeltaLabelsHashRecompute(t *testing.T) {
	// deltaHash == 0 → the result hash is recomputed and matches the stable
	// hash of the effective set
	p := NewPipeline([]Stage{NoopStage})
	sp := p.ForStream(dlBase)
	_, res, ok := sp.Process(0, []byte("l"), dlDelta, 0)
	require.True(t, ok)
	require.Equal(t, dlHash(dlBase, dlDelta), res.Hash())
}

func TestDeltaLabelsCollisionRename(t *testing.T) {
	// a delta name colliding with a stream label is renamed name_extracted
	// and the stored hash is distrusted
	delta := labels.FromStrings("cluster", "other-cluster", "severity_text", "WARN")
	p := NewNoopPipeline()
	sp := p.ForStream(dlBase)
	_, res, ok := sp.Process(0, []byte("l"), delta, 12345 /* bogus stored hash */)
	require.True(t, ok)
	want := labels.FromStrings(
		"app", "nginx", "cluster", "us-central1",
		"cluster_extracted", "other-cluster", "severity_text", "WARN",
	)
	require.Equal(t, want, res.Labels())
	// bogus hash must NOT be trusted on the rename path
	require.Equal(t, labels.StableHash(want), res.Hash())
	require.Equal(t,
		labels.FromStrings("cluster_extracted", "other-cluster", "severity_text", "WARN"),
		res.Delta())
}

func TestDeltaLabelsEmptyValueIgnored(t *testing.T) {
	// empty-valued delta labels do not exist by the frozen format rules;
	// tolerate foreign input by ignoring them (and distrusting the hash)
	b := labels.NewScratchBuilder(2)
	b.Add("empty", "")
	b.Add("kept", "v")
	b.Sort()
	delta := b.Labels()

	p := NewNoopPipeline()
	sp := p.ForStream(dlBase)
	_, res, ok := sp.Process(0, []byte("l"), delta, 999)
	require.True(t, ok)
	want := labels.FromStrings("app", "nginx", "cluster", "us-central1", "kept", "v")
	require.Equal(t, want, res.Labels())
	require.Equal(t, labels.StableHash(want), res.Hash())
}

func TestDeltaLabelsPrecedenceParsedWins(t *testing.T) {
	// json parser extracts severity_text → parsed wins over the delta value
	p := NewPipeline([]Stage{NewJSONParser()})
	sp := p.ForStream(dlBase)
	require.True(t, sp.ReferencedDeltaLabels())

	line := []byte(`{"severity_text":"FROM_PARSER","other":"x"}`)
	_, res, ok := sp.Process(0, line, dlDelta, dlHash(dlBase, dlDelta))
	require.True(t, ok)
	require.Equal(t, "FROM_PARSER", res.Labels().Get("severity_text"))
	require.Equal(t, "x", res.Labels().Get("other"))
	// trace_id from the delta is untouched
	require.Equal(t, "0af7651916cd43dd8448eb211c80319c", res.Labels().Get("trace_id"))
	// categorization: severity_text moved to parsed, only trace_id remains delta
	require.Equal(
		t,
		labels.FromStrings("trace_id", "0af7651916cd43dd8448eb211c80319c"),
		res.Delta(),
	)
	require.Equal(t, "FROM_PARSER", res.Parsed().Get("severity_text"))
	require.Equal(t, "x", res.Parsed().Get("other"))
}

func TestDeltaLabelsFilterOnDelta(t *testing.T) {
	// label filter on a delta label
	f := NewStringLabelFilter(labels.MustNewMatcher(labels.MatchEqual, "severity_text", "ERROR"))
	p := NewPipeline([]Stage{f})
	sp := p.ForStream(dlBase)
	require.True(t, sp.ReferencedDeltaLabels())

	_, _, ok := sp.Process(0, []byte("l"), dlDelta, 0)
	require.True(t, ok)

	warnDelta := labels.FromStrings("severity_text", "WARN")
	_, _, ok = sp.Process(0, []byte("l"), warnDelta, 0)
	require.False(t, ok)

	// no delta at all → filter does not match either
	_, _, ok = sp.Process(0, []byte("l"), labels.EmptyLabels(), 0)
	require.False(t, ok)
}

func TestDeltaLabelsDeletion(t *testing.T) {
	// drop a stream label and a delta label
	stage := NewDropLabels([]DropLabel{
		{Matcher: nil, Name: "cluster"},
		{Matcher: nil, Name: "trace_id"},
	})
	p := NewPipeline([]Stage{stage})
	sp := p.ForStream(dlBase)
	_, res, ok := sp.Process(0, []byte("l"), dlDelta, dlHash(dlBase, dlDelta))
	require.True(t, ok)
	want := labels.FromStrings("app", "nginx", "severity_text", "ERROR")
	require.Equal(t, want, res.Labels())
	require.Equal(t, labels.StableHash(want), res.Hash())
	require.Equal(t, labels.FromStrings("app", "nginx"), res.Stream())
	require.Equal(t, labels.FromStrings("severity_text", "ERROR"), res.Delta())
	require.Equal(t, []string{"cluster"}, res.Deleted())
}

func TestDeltaLabelsCategorizedNoAliasing(t *testing.T) {
	// two streams whose EFFECTIVE label sets are identical but with different
	// stream/delta decompositions must not share a categorized result
	p := NewNoopPipeline()

	baseA := labels.FromStrings("app", "nginx")
	deltaA := labels.FromStrings("region", "eu")
	spA := p.ForStream(baseA)
	_, resA, _ := spA.Process(0, []byte("l"), deltaA, 0)

	baseB := labels.FromStrings("app", "nginx", "region", "eu")
	spB := p.ForStream(baseB)
	_, resB, _ := spB.Process(0, []byte("l"), labels.EmptyLabels(), 0)

	require.Equal(t, resA.Labels(), resB.Labels())
	require.Equal(t, resA.Hash(), resB.Hash())
	require.NotSame(t, resA, resB)
	require.Equal(t, baseA, resA.Stream())
	require.Equal(t, deltaA, resA.Delta())
	require.Equal(t, baseB, resB.Stream())
	require.True(t, resB.Delta().IsEmpty())
}

func TestDeltaLabelsGrouping(t *testing.T) {
	// sum by (severity_text): grouping by a delta label
	ex, err := NewLineSampleExtractor(CountExtractor, nil, []string{"severity_text"}, false, false)
	require.NoError(t, err)
	se := ex.ForStream(dlBase)
	require.True(t, se.ReferencedDeltaLabels())

	v, res, ok := se.Process(0, []byte("l"), dlDelta, dlHash(dlBase, dlDelta))
	require.True(t, ok)
	require.Equal(t, 1.0, v)
	require.Equal(t, labels.FromStrings("severity_text", "ERROR"), res.Labels())

	// without(severity_text): delta labels flow into the kept set
	exW, err := NewLineSampleExtractor(CountExtractor, nil, []string{"severity_text"}, true, false)
	require.NoError(t, err)
	seW := exW.ForStream(dlBase)
	_, resW, ok := seW.Process(0, []byte("l"), dlDelta, 0)
	require.True(t, ok)
	require.Equal(t, labels.FromStrings(
		"app", "nginx", "cluster", "us-central1",
		"trace_id", "0af7651916cd43dd8448eb211c80319c",
	), resW.Labels())

	// no grouping, no stages: delta must still appear in the sample labels
	exN, err := NewLineSampleExtractor(CountExtractor, nil, nil, false, false)
	require.NoError(t, err)
	seN := exN.ForStream(dlBase)
	_, resN, ok := seN.Process(0, []byte("l"), dlDelta, 0)
	require.True(t, ok)
	require.Equal(t, dlHash(dlBase, dlDelta), resN.Hash())
	// followed by a delta-less line on the same (shared) builder: no stale
	// delta state may leak into the result
	_, resPlain, ok := seN.Process(0, []byte("l"), labels.EmptyLabels(), 0)
	require.True(t, ok)
	require.Equal(t, dlBase, resPlain.Labels())
}

func TestDeltaLabelsUnwrap(t *testing.T) {
	// unwrap a delta label: | unwrap duration_ms
	ex, err := LabelExtractorWithStages(
		"duration_ms", ConvertFloat, nil, false, false, nil, NoopStage,
	)
	require.NoError(t, err)
	se := ex.ForStream(dlBase)
	require.True(t, se.ReferencedDeltaLabels())

	delta := labels.FromStrings("duration_ms", "42.5")
	v, res, ok := se.Process(0, []byte("l"), delta, 0)
	require.True(t, ok)
	require.Equal(t, 42.5, v)
	require.False(t, res.Labels().Has(ErrorLabel))

	// missing delta on the next line → extraction error label
	_, res, ok = se.Process(0, []byte("l"), labels.EmptyLabels(), 0)
	require.True(t, ok)
	require.Equal(t, errSampleExtraction, res.Labels().Get(ErrorLabel))
}

func TestReferencedDeltaLabels(t *testing.T) {
	// noop pipeline → false
	require.False(t, NewNoopPipeline().ForStream(dlBase).ReferencedDeltaLabels())

	// line-filter-only pipeline → false (store may pass delta blobs through)
	f, err := NewFilter("err", labels.MatchEqual)
	require.NoError(t, err)
	pf := NewPipeline([]Stage{f.ToStage()})
	require.False(t, pf.ForStream(dlBase).ReferencedDeltaLabels())
	// ... but the deltas still flow into the results
	_, res, ok := pf.ForStream(dlBase).Process(0, []byte("some err line"), dlDelta, 0)
	require.True(t, ok)
	require.Equal(t, "ERROR", res.Labels().Get("severity_text"))

	// parser → true
	pp := NewPipeline([]Stage{NewJSONParser()})
	require.True(t, pp.ForStream(dlBase).ReferencedDeltaLabels())

	// count extractor with line filter only and noLabels → false
	exNoLabels, err := NewLineSampleExtractor(
		CountExtractor, []Stage{f.ToStage()}, nil, false, true,
	)
	require.NoError(t, err)
	require.False(t, exNoLabels.ForStream(dlBase).ReferencedDeltaLabels())
}

func TestDeltaLabelsLineFormatTemplate(t *testing.T) {
	// line_format templates must see delta labels ({{.severity_text}})
	lf, err := NewFormatter("{{.severity_text}}:{{.app}}")
	require.NoError(t, err)
	p := NewPipeline([]Stage{lf})
	sp := p.ForStream(dlBase)
	out, _, ok := sp.Process(0, []byte("original"), dlDelta, 0)
	require.True(t, ok)
	require.Equal(t, "ERROR:nginx", string(out))

	// and a delta-less line right after on the same builder sees no leak
	out, _, ok = sp.Process(0, []byte("original"), labels.EmptyLabels(), 0)
	require.True(t, ok)
	require.Equal(t, ":nginx", string(out))
}

func TestDeltaLabelsLabelFormatRename(t *testing.T) {
	// label_format renaming a delta label: rename reads the delta value and
	// deletes the source name
	rename, err := NewLabelsFormatter([]LabelFmt{NewRenameLabelFmt("sev", "severity_text")})
	require.NoError(t, err)
	p := NewPipeline([]Stage{rename})
	sp := p.ForStream(dlBase)
	_, res, ok := sp.Process(0, []byte("l"), dlDelta, 0)
	require.True(t, ok)
	require.Equal(t, "ERROR", res.Labels().Get("sev"))
	require.False(t, res.Labels().Has("severity_text"))
	// the renamed value was produced by a stage → parsed category
	require.Equal(t, "ERROR", res.Parsed().Get("sev"))
}
