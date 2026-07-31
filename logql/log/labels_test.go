package log

import (
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestLabelsBuilder_Get(t *testing.T) {
	lbs := labels.FromStrings("already", "in")
	b := NewBaseLabelsBuilder().ForLabels(lbs, labels.StableHash(lbs))
	b.Reset()
	b.Set("foo", "bar")
	b.Set("bar", "buzz")
	b.Del("foo")
	_, ok := b.Get("foo")
	require.False(t, ok)
	v, ok := b.Get("bar")
	require.True(t, ok)
	require.Equal(t, "buzz", v)
	v, ok = b.Get("already")
	require.True(t, ok)
	require.Equal(t, "in", v)
	b.Del("bar")
	_, ok = b.Get("bar")
	require.False(t, ok)
	b.Del("already")
	_, ok = b.Get("already")
	require.False(t, ok)
}

func TestLabelsBuilder_LabelsError(t *testing.T) {
	lbs := labels.FromStrings("already", "in")
	b := NewBaseLabelsBuilder().ForLabels(lbs, labels.StableHash(lbs))
	b.Reset()
	b.SetErr("err")
	lbsWithErr := b.LabelsResult().Labels()
	require.Equal(
		t,
		labels.FromStrings(ErrorLabel, "err", "already", "in"),
		lbsWithErr,
	)
	// make sure the original labels is unchanged.
	require.Equal(t, labels.FromStrings("already", "in"), lbs)
}

func TestLabelsBuilder_LabelsResult(t *testing.T) {
	lbs := labels.FromStrings(
		"namespace",
		"loki",
		"job",
		"us-central1/loki",
		"cluster",
		"us-central1",
	)
	b := NewBaseLabelsBuilder().ForLabels(lbs, labels.StableHash(lbs))
	b.Reset()
	assertLabelResult(t, lbs, b.LabelsResult())
	b.SetErr("err")
	withErr := labels.NewBuilder(lbs).Set(ErrorLabel, "err").Labels()
	assertLabelResult(t, withErr, b.LabelsResult())

	b.Set("foo", "bar")
	b.Set("namespace", "tempo")
	b.Set("buzz", "fuzz")
	b.Del("job")
	expected := labels.FromStrings(
		ErrorLabel,
		"err",
		"namespace",
		"tempo",
		"cluster",
		"us-central1",
		"foo",
		"bar",
		"buzz",
		"fuzz",
	)
	assertLabelResult(t, expected, b.LabelsResult())
	// cached.
	assertLabelResult(t, expected, b.LabelsResult())
}

func TestLabelsBuilder_GroupedLabelsResult(t *testing.T) {
	lbs := labels.FromStrings(
		"namespace",
		"loki",
		"job",
		"us-central1/loki",
		"cluster",
		"us-central1",
	)
	b := NewBaseLabelsBuilderWithGrouping(
		[]string{"namespace"},
		nil,
		false,
		false,
	).ForLabels(lbs, labels.StableHash(lbs))
	b.Reset()
	assertLabelResult(
		t,
		labels.FromStrings("namespace", "loki"),
		b.GroupedLabels(),
	)
	b.SetErr("err")
	withErr := labels.NewBuilder(lbs).Set(ErrorLabel, "err").Labels()
	assertLabelResult(t, withErr, b.GroupedLabels())

	b.Reset()
	b.Set("foo", "bar")
	b.Set("namespace", "tempo")
	b.Set("buzz", "fuzz")
	b.Del("job")
	expected := labels.FromStrings("namespace", "tempo")
	assertLabelResult(t, expected, b.GroupedLabels())
	// cached.
	assertLabelResult(t, expected, b.GroupedLabels())

	b = NewBaseLabelsBuilderWithGrouping(
		[]string{"job"},
		nil,
		false,
		false,
	).ForLabels(lbs, labels.StableHash(lbs))
	assertLabelResult(
		t,
		labels.FromStrings("job", "us-central1/loki"),
		b.GroupedLabels(),
	)
	assertLabelResult(
		t,
		labels.FromStrings("job", "us-central1/loki"),
		b.GroupedLabels(),
	)
	b.Del("job")
	assertLabelResult(t, labels.EmptyLabels(), b.GroupedLabels())
	b.Reset()
	b.Set("namespace", "tempo")
	assertLabelResult(
		t,
		labels.FromStrings("job", "us-central1/loki"),
		b.GroupedLabels(),
	)

	b = NewBaseLabelsBuilderWithGrouping(
		[]string{"job"},
		nil,
		true,
		false,
	).ForLabels(lbs, labels.StableHash(lbs))
	b.Del("job")
	b.Set("foo", "bar")
	b.Set("job", "something")
	expected = labels.FromStrings("namespace", "loki", "cluster", "us-central1", "foo", "bar")
	assertLabelResult(t, expected, b.GroupedLabels())

	b = NewBaseLabelsBuilderWithGrouping(
		nil,
		nil,
		false,
		false,
	).ForLabels(lbs, labels.StableHash(lbs))
	b.Set("foo", "bar")
	b.Set("job", "something")
	expected = labels.FromStrings(
		"namespace",
		"loki",
		"job",
		"something",
		"cluster",
		"us-central1",
		"foo",
		"bar",
	)
	assertLabelResult(t, expected, b.GroupedLabels())
}

// Streams that group down to the same label set must share a single
// LabelsResult: toBaseGroup goes through the base builder's result cache, so a
// query grouping N streams retains one result per distinct group instead of
// one per stream (under stringlabels every materialized labels.Labels owns a
// private copy of its bytes, so that difference is real memory).
func TestLabelsBuilder_GroupedLabelsResultIsShared(t *testing.T) {
	lbs1 := labels.FromStrings("cluster", "us-central1", "namespace", "loki", "pod", "a")
	lbs2 := labels.FromStrings("cluster", "us-central1", "namespace", "loki", "pod", "b")

	for _, tt := range []struct {
		name     string
		groups   []string
		without  bool
		expected labels.Labels
	}{
		{
			name:     "by",
			groups:   []string{"namespace"},
			expected: labels.FromStrings("namespace", "loki"),
		},
		{
			name:     "without",
			groups:   []string{"pod"},
			without:  true,
			expected: labels.FromStrings("cluster", "us-central1", "namespace", "loki"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base := NewBaseLabelsBuilderWithGrouping(tt.groups, nil, tt.without, false)
			res1 := base.ForLabels(lbs1, labels.StableHash(lbs1)).GroupedLabels()
			res2 := base.ForLabels(lbs2, labels.StableHash(lbs2)).GroupedLabels()

			assertLabelResult(t, tt.expected, res1)
			assertLabelResult(t, tt.expected, res2)
			require.Same(t, res1, res2)
		})
	}
}

func assertLabelResult(t *testing.T, lbs labels.Labels, res LabelsResult) {
	t.Helper()
	require.Equal(t,
		lbs,
		res.Labels(),
	)
	require.Equal(t,
		labels.StableHash(lbs),
		res.Hash(),
	)
	require.Equal(t,
		lbs.String(),
		res.String(),
	)
}

// Benchmark_GroupedLabels_ManyStreams measures the no-stage grouping path
// (streamLineSampleExtractor.Process on a NoopStage, i.e. every plain
// `sum by(...) (count_over_time(...))` alerting query): one LabelsBuilder per
// stream, GroupedLabels() once per line.
func Benchmark_GroupedLabels_ManyStreams(b *testing.B) {
	const nbStreams = 1000

	streams := make([]labels.Labels, nbStreams)
	for i := range streams {
		streams[i] = labels.FromStrings(
			"cluster", "us-central1",
			"namespace", "loki",
			"job", "us-central1/loki",
			"container", "querier",
			"pod", "querier-5896759c79-q7q9h-"+strconv.Itoa(i),
			"path", "/var/log/pods/loki/querier/"+strconv.Itoa(i)+"/0.log",
		)
	}

	for _, tt := range []struct {
		name    string
		groups  []string
		without bool
	}{
		{name: "by", groups: []string{"cluster", "namespace"}},
		{name: "without", groups: []string{"path", "pod"}, without: true},
	} {
		// one iteration == one query grouping the whole stream set, which is
		// what the per-stream cost is paid on (GroupedLabels caches its result
		// per stream afterwards).
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				base := NewBaseLabelsBuilderWithGrouping(tt.groups, nil, tt.without, false)
				for _, s := range streams {
					resLbs = base.ForLabels(s, labels.StableHash(s)).GroupedLabels()
				}
			}
		})
	}
}
