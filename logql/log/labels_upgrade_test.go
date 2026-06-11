package log

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

// Regression tests for the migration to the modern prometheus model/labels
// API (stringlabels representation). They pin the builder semantics, the
// empty-value behavior, the LabelsResult hash consistency / caching and the
// response-visible LabelsResult.String() format.

func TestLabelsBuilder_SemanticsTable(t *testing.T) {
	base := labels.FromStrings("cluster", "us-east", "job", "loki", "namespace", "prod")

	for _, tt := range []struct {
		name  string
		build func(b *LabelsBuilder)
		want  labels.Labels
	}{
		{
			"no changes",
			func(b *LabelsBuilder) {},
			base,
		},
		{
			"set new labels are sorted into place",
			func(b *LabelsBuilder) { b.Set("zzz", "1"); b.Set("aaa", "2") },
			labels.FromStrings(
				"aaa", "2",
				"cluster", "us-east",
				"job", "loki",
				"namespace", "prod",
				"zzz", "1",
			),
		},
		{
			"override base label",
			func(b *LabelsBuilder) { b.Set("job", "tempo") },
			labels.FromStrings("cluster", "us-east", "job", "tempo", "namespace", "prod"),
		},
		{
			"del base label",
			func(b *LabelsBuilder) { b.Del("namespace") },
			labels.FromStrings("cluster", "us-east", "job", "loki"),
		},
		{
			"del after set removes the label",
			func(b *LabelsBuilder) { b.Set("foo", "bar"); b.Del("foo") },
			base,
		},
		{
			"set after del re-adds the label",
			func(b *LabelsBuilder) { b.Del("job"); b.Set("job", "tempo") },
			labels.FromStrings("cluster", "us-east", "job", "tempo", "namespace", "prod"),
		},
		{
			"set empty value deletes base label",
			func(b *LabelsBuilder) { b.Set("job", "") },
			labels.FromStrings("cluster", "us-east", "namespace", "prod"),
		},
		{
			"set empty value on new label is a no-op",
			func(b *LabelsBuilder) { b.Set("foo", "") },
			base,
		},
		{
			"error label is added",
			func(b *LabelsBuilder) { b.SetErr("boom") },
			labels.FromStrings(
				ErrorLabel, "boom",
				"cluster", "us-east",
				"job", "loki",
				"namespace", "prod",
			),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(base, labels.StableHash(base))
			b.Reset()
			tt.build(b)
			res := b.LabelsResult()
			require.Equal(t, tt.want, res.Labels())
			// the hash must stay consistent with the stable hash of the
			// resulting label set (none of these sets carries __name__).
			require.Equal(t, labels.StableHash(tt.want), res.Hash())
			require.Equal(t, tt.want.String(), res.String())
		})
	}
}

func TestLabelsBuilder_GetAfterSetDel(t *testing.T) {
	base := labels.FromStrings("job", "loki")
	b := NewBaseLabelsBuilder().ForLabels(base, labels.StableHash(base))
	b.Reset()

	v, ok := b.Get("job")
	require.True(t, ok)
	require.Equal(t, "loki", v)

	b.Set("job", "")
	_, ok = b.Get("job")
	require.False(t, ok, "Set with empty value must behave like Del")

	b.Set("foo", "bar")
	v, ok = b.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar", v)
}

func TestLabelsResult_CacheHitsReturnSameInstance(t *testing.T) {
	base := labels.FromStrings("job", "loki")
	bb := NewBaseLabelsBuilder()

	b := bb.ForLabels(base, labels.StableHash(base))
	b.Reset()
	b.Set("foo", "bar")
	res1 := b.LabelsResult()

	b.Reset()
	b.Set("foo", "bar")
	res2 := b.LabelsResult()
	require.Same(t, res1, res2, "same modifications must hit the result cache")

	// a different builder sharing the same base builder hits the same cache.
	b2 := bb.ForLabels(base, labels.StableHash(base))
	b2.Reset()
	b2.Set("foo", "bar")
	require.Same(t, res1, b2.LabelsResult())

	// the unchanged path returns the cached current result.
	b.Reset()
	require.Same(t, b.LabelsResult(), b2.LabelsResult())
}

func TestLabelsResult_GroupedHashConsistency(t *testing.T) {
	base := labels.FromStrings("cluster", "us-east", "job", "loki", "namespace", "prod")

	// by (cluster, job) with a modification, groups sorted as the engine does.
	b := NewBaseLabelsBuilderWithGrouping([]string{"cluster", "job"}, nil, false, false).
		ForLabels(base, labels.StableHash(base))
	b.Reset()
	b.Set("job", "tempo")
	res := b.GroupedLabels()
	want := labels.FromStrings("cluster", "us-east", "job", "tempo")
	require.Equal(t, want, res.Labels())
	require.Equal(t, labels.StableHash(want), res.Hash())

	// without (job) with a modification.
	b = NewBaseLabelsBuilderWithGrouping([]string{"job"}, nil, true, false).
		ForLabels(base, labels.StableHash(base))
	b.Reset()
	b.Set("foo", "bar")
	res = b.GroupedLabels()
	want = labels.FromStrings("cluster", "us-east", "foo", "bar", "namespace", "prod")
	require.Equal(t, want, res.Labels())
	require.Equal(t, labels.StableHash(want), res.Hash())
}

// TestEmptyValue_StagesDropLabels asserts the upstream-aligned behavior:
// stages producing an empty label value leave the label absent.
func TestEmptyValue_StagesDropLabels(t *testing.T) {
	base := labels.FromStrings("job", "loki")

	t.Run("label_format empty template result", func(t *testing.T) {
		f, err := NewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("empty", "{{ .missing }}")})
		require.NoError(t, err)
		b := NewBaseLabelsBuilder().ForLabels(base, labels.StableHash(base))
		b.Reset()
		_, ok := f.Process(0, []byte("line"), b)
		require.True(t, ok)
		require.Equal(t, base, b.LabelsResult().Labels(), "empty label must be absent")
	})

	t.Run("json parser empty value", func(t *testing.T) {
		b := NewBaseLabelsBuilder().ForLabels(base, labels.StableHash(base))
		b.Reset()
		_, ok := NewJSONParser().Process(0, []byte(`{"empty":"","filled":"x"}`), b)
		require.True(t, ok)
		require.Equal(
			t,
			labels.FromStrings("filled", "x", "job", "loki"),
			b.LabelsResult().Labels(),
		)
	})

	t.Run("logfmt parser empty value", func(t *testing.T) {
		b := NewBaseLabelsBuilder().ForLabels(base, labels.StableHash(base))
		b.Reset()
		_, ok := NewLogfmtParser().Process(0, []byte(`empty= filled=x`), b)
		require.True(t, ok)
		require.Equal(
			t,
			labels.FromStrings("filled", "x", "job", "loki"),
			b.LabelsResult().Labels(),
		)
	})
}

// TestPipeline_LabelsResultStringFormat pins the response-visible string
// format of LabelsResult: sorted, `, ` separated, values quoted like
// strconv.Quote.
func TestPipeline_LabelsResultStringFormat(t *testing.T) {
	base := labels.FromStrings("job", "loki", "cluster", "us-east")

	// unchanged path (noop pipeline).
	_, res, ok := NewNoopPipeline().ForStream(base).Process(0, []byte("line"))
	require.True(t, ok)
	require.Equal(t, `{cluster="us-east", job="loki"}`, res.String())

	// modified path through a real pipeline stage.
	f, err := NewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("foo", "bar")})
	require.NoError(t, err)
	sp := NewPipeline([]Stage{f}).ForStream(base)
	_, res, ok = sp.Process(0, []byte("line"))
	require.True(t, ok)
	require.Equal(t, `{cluster="us-east", foo="bar", job="loki"}`, res.String())

	// quoting of special characters matches strconv.Quote.
	special := labels.FromStrings("msg", "he said \"hi\"\n", "job", "loki")
	_, res, ok = NewNoopPipeline().ForStream(special).Process(0, []byte("line"))
	require.True(t, ok)
	require.Equal(t, `{job="loki", msg="he said \"hi\"\n"}`, res.String())
}
