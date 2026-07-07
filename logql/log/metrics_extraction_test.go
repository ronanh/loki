package log

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test_labelSampleExtractor_Extract(t *testing.T) {
	tests := []struct {
		name    string
		ex      SampleExtractor
		in      labels.Labels
		want    float64
		wantLbs labels.Labels
		wantOk  bool
	}{
		{
			"convert float",
			mustSampleExtractor(LabelExtractorWithStages(
				"foo", ConvertFloat, nil, false, false, nil, NoopStage,
			)),
			labels.FromStrings("foo", "15.0"),
			15,
			labels.EmptyLabels(),
			true,
		},
		{
			"convert float as vector with no grouping",
			mustSampleExtractor(LabelExtractorWithStages(
				"foo", ConvertFloat, nil, false, true, nil, NoopStage,
			)),
			labels.FromStrings("foo", "15.0", "bar", "buzz"),
			15,
			labels.EmptyLabels(),
			true,
		},
		{
			"convert float without",
			mustSampleExtractor(LabelExtractorWithStages(
				"foo", ConvertFloat, []string{"bar", "buzz"}, true, false, nil, NoopStage,
			)),
			labels.FromStrings("foo", "10", "bar", "foo", "buzz", "blip", "namespace", "dev"),
			10,
			labels.FromStrings("namespace", "dev"),
			true,
		},
		{
			"convert float with",
			mustSampleExtractor(LabelExtractorWithStages(
				"foo", ConvertFloat, []string{"bar", "buzz"}, false, false, nil, NoopStage,
			)),
			labels.FromStrings("foo", "0.6", "bar", "foo", "buzz", "blip", "namespace", "dev"),
			0.6,
			labels.FromStrings("bar", "foo", "buzz", "blip"),
			true,
		},
		{
			"convert duration with",
			mustSampleExtractor(LabelExtractorWithStages(
				"foo", ConvertDuration, []string{"bar", "buzz"}, false, false, nil, NoopStage,
			)),
			labels.FromStrings("foo", "500ms", "bar", "foo", "buzz", "blip", "namespace", "dev"),
			0.5,
			labels.FromStrings("bar", "foo", "buzz", "blip"),
			true,
		},
		{
			"convert bytes",
			mustSampleExtractor(LabelExtractorWithStages(
				"foo", ConvertBytes, []string{"bar", "buzz"}, false, false, nil, NoopStage,
			)),
			labels.FromStrings("foo", "13 MiB", "bar", "foo", "buzz", "blip", "namespace", "dev"),
			13 * 1024 * 1024,
			labels.FromStrings("bar", "foo", "buzz", "blip"),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			outval, outlbs, ok := tt.ex.ForStream(tt.in).
				Process(0, []byte(""), labels.EmptyLabels(), 0)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.want, outval)
			require.Equal(t, tt.wantLbs, outlbs.Labels())

			outval, outlbs, ok = tt.ex.ForStream(tt.in).
				ProcessString(0, "", labels.EmptyLabels(), 0)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.want, outval)
			require.Equal(t, tt.wantLbs, outlbs.Labels())
		})
	}
}

func Test_Extract_ExpectedLabels(t *testing.T) {
	ex := mustSampleExtractor(
		LabelExtractorWithStages(
			"duration",
			ConvertDuration,
			[]string{"foo"},
			false,
			false,
			[]Stage{NewJSONParser()},
			NoopStage,
		),
	)

	f, lbs, ok := ex.ForStream(labels.FromStrings("bar", "foo")).
		ProcessString(0, `{"duration":"20ms","foo":"json"}`, labels.EmptyLabels(), 0)
	require.True(t, ok)
	require.Equal(t, (20 * time.Millisecond).Seconds(), f)
	require.Equal(t, labels.FromStrings("foo", "json"), lbs.Labels())
}

func mustSampleExtractor(ex SampleExtractor, err error) SampleExtractor {
	if err != nil {
		panic(err)
	}
	return ex
}

func TestNewLineSampleExtractor(t *testing.T) {
	se, err := NewLineSampleExtractor(CountExtractor, nil, nil, false, false)
	require.NoError(t, err)
	lbs := labels.FromStrings("namespace", "dev", "cluster", "us-central1")
	sse := se.ForStream(lbs)
	f, l, ok := sse.Process(0, []byte(`foo`), labels.EmptyLabels(), 0)
	require.True(t, ok)
	require.Equal(t, 1., f)
	assertLabelResult(t, lbs, l)

	f, l, ok = sse.ProcessString(0, `foo`, labels.EmptyLabels(), 0)
	require.True(t, ok)
	require.Equal(t, 1., f)
	assertLabelResult(t, lbs, l)

	filter, err := NewFilter("foo", labels.MatchEqual)
	require.NoError(t, err)

	se, err = NewLineSampleExtractor(
		BytesExtractor,
		[]Stage{filter.ToStage()},
		[]string{"namespace"},
		false,
		false,
	)
	require.NoError(t, err)
	sse = se.ForStream(lbs)
	f, l, ok = sse.Process(0, []byte(`foo`), labels.EmptyLabels(), 0)
	require.True(t, ok)
	require.Equal(t, 3., f)
	assertLabelResult(t, labels.FromStrings("namespace", "dev"), l)
	sse = se.ForStream(lbs)
	_, _, ok = sse.Process(0, []byte(`nope`), labels.EmptyLabels(), 0)
	require.False(t, ok)
}
