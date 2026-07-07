package log

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// Delta-labels benchmark matrix (roadmap WS-B: noop / parser / delta-heavy /
// mixed, with allocation counts). The delta-less variants measure the
// regression on the historic paths; compare against
// bench/delta-labels/baseline.txt for the pre-deltaLabels numbers.

var (
	benchDelta = labels.FromStrings(
		"severity_text", "ERROR",
		"trace_id", "0af7651916cd43dd8448eb211c80319c",
	)
	benchBase = labels.FromStrings(
		"app",
		"nginx",
		"cluster",
		"us-central1",
		"namespace",
		"prod",
	)
	benchLine     = []byte(`level=error msg="something went wrong" duration=1.5s status=500`)
	benchJSONLine = []byte(`{"status":500,"duration":"1.5s","msg":"something went wrong"}`)
)

func benchDeltaHash() uint64 {
	b := labels.NewBuilder(benchBase)
	benchDelta.Range(func(l labels.Label) { b.Set(l.Name, l.Value) })
	return labels.StableHash(b.Labels())
}

// noop pipeline, no delta: must stay on the historic constant-result path.
func Benchmark_DeltaLabels_Noop_NoDelta(b *testing.B) {
	sp := NewNoopPipeline().ForStream(benchBase)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resLine, resLbs, resOK = sp.Process(0, benchLine, labels.EmptyLabels(), 0)
	}
}

// noop pipeline, every line carries the same delta with its stored hash:
// the deltaHash fast path (zero hashing, zero materialization on repeats).
func Benchmark_DeltaLabels_Noop_StoredHash(b *testing.B) {
	sp := NewNoopPipeline().ForStream(benchBase)
	h := benchDeltaHash()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resLine, resLbs, resOK = sp.Process(0, benchLine, benchDelta, h)
	}
}

// noop pipeline, delta without a stored hash: recompute path.
func Benchmark_DeltaLabels_Noop_Recompute(b *testing.B) {
	sp := NewNoopPipeline().ForStream(benchBase)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resLine, resLbs, resOK = sp.Process(0, benchLine, benchDelta, 0)
	}
}

// line-filter pipeline with deltas (the ReferencedDeltaLabels()==false shape).
func Benchmark_DeltaLabels_LineFilter_WithDelta(b *testing.B) {
	f, err := NewFilter("error", labels.MatchEqual)
	if err != nil {
		b.Fatal(err)
	}
	sp := NewPipeline([]Stage{f.ToStage()}).ForStream(benchBase)
	h := benchDeltaHash()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resLine, resLbs, resOK = sp.Process(0, benchLine, benchDelta, h)
	}
}

// json parser without deltas: regression gauge for the parser-heavy path.
func Benchmark_DeltaLabels_Parser_NoDelta(b *testing.B) {
	sp := NewPipeline([]Stage{NewJSONParser()}).ForStream(benchBase)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resLine, resLbs, resOK = sp.Process(0, benchJSONLine, labels.EmptyLabels(), 0)
	}
}

// json parser + deltas: the mixed shape (parsed + delta + stream categories).
func Benchmark_DeltaLabels_Parser_WithDelta(b *testing.B) {
	sp := NewPipeline([]Stage{NewJSONParser()}).ForStream(benchBase)
	h := benchDeltaHash()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resLine, resLbs, resOK = sp.Process(0, benchJSONLine, benchDelta, h)
	}
}

// delta-heavy: every line a different delta value (worst case for the result
// cache — unbounded distinct combinations).
func Benchmark_DeltaLabels_Noop_HighCardinality(b *testing.B) {
	sp := NewNoopPipeline().ForStream(benchBase)
	traces := make([]labels.Labels, 256)
	for i := range traces {
		traces[i] = labels.FromStrings(
			"severity_text", "ERROR",
			"trace_id", string(rune('a'+i%26))+"trace",
		)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resLine, resLbs, resOK = sp.Process(0, benchLine, traces[i%256], 0)
	}
}

// count_over_time extractor with grouping by a delta label.
func Benchmark_DeltaLabels_Extractor_GroupByDelta(b *testing.B) {
	ex, err := NewLineSampleExtractor(CountExtractor, nil, []string{"severity_text"}, false, false)
	if err != nil {
		b.Fatal(err)
	}
	se := ex.ForStream(benchBase)
	h := benchDeltaHash()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resFloat, resLbs, resOK = se.Process(0, benchLine, benchDelta, h)
	}
}

var resFloat float64
