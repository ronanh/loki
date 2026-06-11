package log

import (
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestBinary_Filter(t *testing.T) {
	tests := []struct {
		f   LabelFilterer
		lbs labels.Labels

		want    bool
		wantLbs labels.Labels
	}{
		{
			NewAndLabelFilter(
				NewNumericLabelFilter(LabelFilterEqual, "foo", 5),
				NewDurationLabelFilter(LabelFilterEqual, "bar", 1*time.Second),
			),
			labels.FromStrings("foo", "5", "bar", "1s"),
			true,
			labels.FromStrings("foo", "5", "bar", "1s"),
		},
		{
			NewAndLabelFilter(
				NewNumericLabelFilter(LabelFilterEqual, "foo", 5),
				NewBytesLabelFilter(LabelFilterEqual, "bar", 42000),
			),
			labels.FromStrings("foo", "5", "bar", "42kB"),
			true,
			labels.FromStrings("foo", "5", "bar", "42kB"),
		},
		{
			NewAndLabelFilter(
				NewNumericLabelFilter(LabelFilterEqual, "foo", 5),
				NewDurationLabelFilter(LabelFilterEqual, "bar", 1*time.Second),
			),
			labels.FromStrings("foo", "6", "bar", "1s"),
			false,
			labels.FromStrings("foo", "6", "bar", "1s"),
		},
		{
			NewAndLabelFilter(
				NewNumericLabelFilter(LabelFilterEqual, "foo", 5),
				NewDurationLabelFilter(LabelFilterEqual, "bar", 1*time.Second),
			),
			labels.FromStrings("foo", "5", "bar", "2s"),
			false,
			labels.FromStrings("foo", "5", "bar", "2s"),
		},
		{
			NewAndLabelFilter(
				NewStringLabelFilter(labels.MustNewMatcher(labels.MatchEqual, "foo", "5")),
				NewDurationLabelFilter(LabelFilterEqual, "bar", 1*time.Second),
			),
			labels.FromStrings("foo", "5", "bar", "1s"),
			true,
			labels.FromStrings("foo", "5", "bar", "1s"),
		},
		{
			NewAndLabelFilter(
				NewStringLabelFilter(labels.MustNewMatcher(labels.MatchEqual, "foo", "5")),
				NewDurationLabelFilter(LabelFilterEqual, "bar", 1*time.Second),
			),
			labels.FromStrings("foo", "6", "bar", "1s"),
			false,
			labels.FromStrings("foo", "6", "bar", "1s"),
		},
		{
			NewAndLabelFilter(
				NewOrLabelFilter(
					NewDurationLabelFilter(LabelFilterGreaterThan, "duration", 1*time.Second),
					NewNumericLabelFilter(LabelFilterNotEqual, "status", 200),
				),
				NewStringLabelFilter(labels.MustNewMatcher(labels.MatchNotEqual, "method", "POST")),
			),
			labels.FromStrings("duration", "2s", "status", "200", "method", "GET"),
			true,
			labels.FromStrings("duration", "2s", "status", "200", "method", "GET"),
		},
		{
			NewAndLabelFilter(
				NewOrLabelFilter(
					NewDurationLabelFilter(LabelFilterGreaterThan, "duration", 1*time.Second),
					NewNumericLabelFilter(LabelFilterNotEqual, "status", 200),
				),
				NewStringLabelFilter(labels.MustNewMatcher(labels.MatchNotEqual, "method", "POST")),
			),
			labels.FromStrings("duration", "2s", "status", "200", "method", "POST"),
			false,
			labels.FromStrings("duration", "2s", "status", "200", "method", "POST"),
		},
		{
			NewAndLabelFilter(
				NewOrLabelFilter(
					NewDurationLabelFilter(LabelFilterGreaterThan, "duration", 1*time.Second),
					NewNumericLabelFilter(LabelFilterNotEqual, "status", 200),
				),
				NewStringLabelFilter(labels.MustNewMatcher(labels.MatchNotEqual, "method", "POST")),
			),
			labels.FromStrings("duration", "2s", "status", "500", "method", "POST"),
			false,
			labels.FromStrings("duration", "2s", "status", "500", "method", "POST"),
		},
		{
			NewAndLabelFilter(
				NewOrLabelFilter(
					NewDurationLabelFilter(LabelFilterGreaterThan, "duration", 3*time.Second),
					NewNumericLabelFilter(LabelFilterNotEqual, "status", 200),
				),
				NewStringLabelFilter(labels.MustNewMatcher(labels.MatchNotEqual, "method", "POST")),
			),
			labels.FromStrings("duration", "2s", "status", "200", "method", "POST"),
			false,
			labels.FromStrings("duration", "2s", "status", "200", "method", "POST"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.f.String(), func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			_, got := tt.f.Process(0, nil, b)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantLbs, b.LabelsResult().Labels())
		})
	}
}

func TestBytes_Filter(t *testing.T) {
	tests := []struct {
		expectedBytes uint64
		label         string

		want      bool
		wantLabel string
	}{
		{42, "42B", true, "42B"},
		{42 * 1000, "42kB", true, "42kB"},
		{42 * 1000 * 1000, "42MB", true, "42MB"},
		{42 * 1000 * 1000 * 1000, "42GB", true, "42GB"},
		{42 * 1000 * 1000 * 1000 * 1000, "42TB", true, "42TB"},
		{42 * 1000 * 1000 * 1000 * 1000 * 1000, "42PB", true, "42PB"},
		{42 * 1024, "42KiB", true, "42KiB"},
		{42 * 1024 * 1024, "42MiB", true, "42MiB"},
		{42 * 1024 * 1024 * 1024, "42GiB", true, "42GiB"},
		{42 * 1024 * 1024 * 1024 * 1024, "42TiB", true, "42TiB"},
		{42 * 1024 * 1024 * 1024 * 1024 * 1024, "42PiB", true, "42PiB"},
	}
	for _, tt := range tests {
		f := NewBytesLabelFilter(LabelFilterEqual, "bar", tt.expectedBytes)
		lbs := labels.FromStrings("bar", tt.label)
		t.Run(f.String(), func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(lbs, labels.StableHash(lbs))
			b.Reset()
			_, got := f.Process(0, nil, b)
			require.Equal(t, tt.want, got)
			wantLbs := labels.FromStrings("bar", tt.wantLabel)
			require.Equal(t, wantLbs, b.LabelsResult().Labels())
		})
	}
}

func TestErrorFiltering(t *testing.T) {
	tests := []struct {
		f   LabelFilterer
		lbs labels.Labels
		err string

		want    bool
		wantLbs labels.Labels
	}{
		{
			NewStringLabelFilter(labels.MustNewMatcher(labels.MatchNotEqual, ErrorLabel, errJSON)),
			labels.FromStrings("status", "200", "method", "POST"),
			errJSON,
			false,
			labels.FromStrings(ErrorLabel, errJSON, "status", "200", "method", "POST"),
		},
		{
			NewStringLabelFilter(labels.MustNewMatcher(labels.MatchNotRegexp, ErrorLabel, ".+")),
			labels.FromStrings("status", "200", "method", "POST"),
			"foo",
			false,
			labels.FromStrings(ErrorLabel, "foo", "status", "200", "method", "POST"),
		},
		{
			NewStringLabelFilter(labels.MustNewMatcher(labels.MatchNotRegexp, ErrorLabel, ".+")),
			labels.FromStrings("status", "200", "method", "POST"),
			"",
			true,
			labels.FromStrings("status", "200", "method", "POST"),
		},
		{
			NewStringLabelFilter(labels.MustNewMatcher(labels.MatchNotEqual, ErrorLabel, errJSON)),
			labels.FromStrings("status", "200", "method", "POST"),
			"",
			true,
			labels.FromStrings("status", "200", "method", "POST"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.f.String(), func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			b.SetErr(tt.err)
			_, got := tt.f.Process(0, nil, b)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantLbs, b.LabelsResult().Labels())
		})
	}
}

func TestReduceAndLabelFilter(t *testing.T) {
	tests := []struct {
		name    string
		filters []LabelFilterer
		want    LabelFilterer
	}{
		{"empty", nil, NoopLabelFilter},
		{
			"1",
			[]LabelFilterer{NewBytesLabelFilter(LabelFilterEqual, "foo", 5)},
			NewBytesLabelFilter(LabelFilterEqual, "foo", 5),
		},
		{
			"2",
			[]LabelFilterer{
				NewBytesLabelFilter(LabelFilterEqual, "foo", 5),
				NewBytesLabelFilter(LabelFilterGreaterThanOrEqual, "bar", 6),
			},
			NewAndLabelFilter(
				NewBytesLabelFilter(LabelFilterEqual, "foo", 5),
				NewBytesLabelFilter(LabelFilterGreaterThanOrEqual, "bar", 6),
			),
		},
		{
			"3",
			[]LabelFilterer{
				NewBytesLabelFilter(LabelFilterEqual, "foo", 5),
				NewBytesLabelFilter(LabelFilterGreaterThanOrEqual, "bar", 6),
				NewStringLabelFilter(labels.MustNewMatcher(labels.MatchEqual, "buzz", "bla")),
			},
			NewAndLabelFilter(
				NewAndLabelFilter(
					NewBytesLabelFilter(LabelFilterEqual, "foo", 5),
					NewBytesLabelFilter(LabelFilterGreaterThanOrEqual, "bar", 6),
				),
				NewStringLabelFilter(labels.MustNewMatcher(labels.MatchEqual, "buzz", "bla")),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReduceAndLabelFilter(tt.filters); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReduceAndLabelFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
