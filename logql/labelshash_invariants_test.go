package logql

// Frozen invariants of the upcoming prometheus labels migration (RFC
// supervision-team/rfcs!4, !80), engine side. HashLabels / HashWithoutLabels /
// HashForLabels key the per-step grouping in vectorAggEvaluator, and
// ParseLabels parses the label set a query is scoped to; the migration moves
// all four from []Label indexing to a packed-string decode and must not move a
// single value.
//
// Snapshots were recorded on PRE-migration code. A diff means STOP the
// migration and flag it — not a re-record. snaps.Update(false) makes a
// repo-wide UPDATE_SNAPS=true a no-op here, and go:test fails on a dirty
// __snapshots__ tree.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
)

var frozenSnapshot = snaps.WithConfig(snaps.Update(false))

var hashFixtures = []struct {
	name string
	lbs  labels.Labels
	// names must be sorted: all three functions merge-join against them.
	names []string
}{
	{
		"typical-k8s",
		labels.FromStrings(
			"cluster", "eu1-c01",
			"environment", "prod",
			"k8s_pod_name", "app-7f9c4d-x2x4z",
			"logtype", "applog",
			"service_id", "svc-123",
		),
		[]string{"cluster", "logtype"},
	},
	{
		"names-not-present",
		labels.FromStrings("job", "loki"),
		[]string{"absent", "missing"},
	},
	{
		"names-empty",
		labels.FromStrings("a", "1", "b", "2"),
		nil,
	},
	{
		"names-cover-everything",
		labels.FromStrings("a", "1", "b", "2"),
		[]string{"a", "b"},
	},
	{
		"with-metric-name",
		labels.FromStrings("__name__", "up", "instance", "10.0.0.1:9090", "job", "prom"),
		[]string{"job"},
	},
	{
		"special-chars",
		labels.FromStrings("msg", "a\"b\\c\nd", "note", "héllo ✓"),
		[]string{"msg"},
	},
	{
		// the separator itself appearing inside a value must not be able to
		// forge a different label set's serialization.
		"value-contains-separator",
		labels.FromStrings("a", "x\xffy", "b", "z"),
		[]string{"a"},
	},
}

func TestLabelsHashInvariants(t *testing.T) {
	for _, f := range hashFixtures {
		t.Run(f.name, func(t *testing.T) {
			var b strings.Builder
			fmt.Fprintf(&b, "labels: %s\n", f.lbs.String())
			fmt.Fprintf(&b, "names:  %q\n", f.names)

			h, buf := HashLabels(nil, f.lbs)
			fmt.Fprintf(&b, "hashLabels:        %#016x\n", h)
			fmt.Fprintf(&b, "  serialized:      %q\n", string(buf))

			h, buf = HashWithoutLabels(nil, f.lbs, f.names...)
			fmt.Fprintf(&b, "hashWithoutLabels: %#016x\n", h)
			fmt.Fprintf(&b, "  serialized:      %q\n", string(buf))

			h, buf = HashForLabels(nil, f.lbs, f.names...)
			fmt.Fprintf(&b, "hashForLabels:     %#016x\n", h)
			fmt.Fprintf(&b, "  serialized:      %q\n", string(buf))

			frozenSnapshot.MatchSnapshot(t, b.String())
		})
	}
}

func TestParseLabelsInvariants(t *testing.T) {
	for _, q := range []string{
		`{job="loki"}`,
		`{cluster="eu1-c01", logtype="applog", service_id="svc-123"}`,
		`{msg="a\"b\\c\nd"}`,
		`{unsorted="1", asorted="2"}`,
	} {
		t.Run(q, func(t *testing.T) {
			lbs, err := ParseLabels(q)
			require.NoError(t, err)

			h, _ := HashLabels(nil, lbs)
			var b strings.Builder
			fmt.Fprintf(&b, "input:  %s\n", q)
			fmt.Fprintf(&b, "parsed: %s\n", lbs.String())
			fmt.Fprintf(&b, "hash:   %#016x\n", h)
			frozenSnapshot.MatchSnapshot(t, b.String())
		})
	}
}

// Structural invariants.

// TestHashBufferReuseIsTransparent pins that passing a dirty buffer in — which
// every caller does, that is the whole point of the []byte parameter — cannot
// change the hash.
func TestHashBufferReuseIsTransparent(t *testing.T) {
	for _, f := range hashFixtures {
		t.Run(f.name, func(t *testing.T) {
			dirty := []byte("leftover bytes from a previous, longer label set")

			want, _ := HashLabels(nil, f.lbs)
			got, _ := HashLabels(dirty, f.lbs)
			require.Equal(t, want, got, "HashLabels")

			want, _ = HashWithoutLabels(nil, f.lbs, f.names...)
			got, _ = HashWithoutLabels(dirty, f.lbs, f.names...)
			require.Equal(t, want, got, "HashWithoutLabels")

			want, _ = HashForLabels(nil, f.lbs, f.names...)
			got, _ = HashForLabels(dirty, f.lbs, f.names...)
			require.Equal(t, want, got, "HashForLabels")
		})
	}
}

// TestHashForAndWithoutLabelsPartition asserts the two are complements: for
// every fixture, keeping names and dropping names must together account for
// exactly the full label set, which is what makes them safe to use as the two
// halves of by()/without() grouping.
func TestHashForAndWithoutLabelsPartition(t *testing.T) {
	for _, f := range hashFixtures {
		t.Run(f.name, func(t *testing.T) {
			_, kept := HashForLabels(nil, f.lbs, f.names...)
			_, dropped := HashWithoutLabels(nil, f.lbs, f.names...)
			_, all := HashLabels(nil, f.lbs)

			require.Equal(t, len(all), len(kept)+len(dropped),
				"kept and dropped must partition the serialized label set")
		})
	}
}

// TestParseLabelsRoundTrips pins that ParseLabels accepts its own rendering.
func TestParseLabelsRoundTrips(t *testing.T) {
	for _, f := range hashFixtures {
		t.Run(f.name, func(t *testing.T) {
			if f.lbs.Get("__name__") != "" {
				t.Skip("__name__ is not part of the {…} selector syntax")
			}
			again, err := ParseLabels(f.lbs.String())
			require.NoError(t, err)
			require.Equal(t, f.lbs.String(), again.String())

			want, _ := HashLabels(nil, f.lbs)
			got, _ := HashLabels(nil, again)
			require.Equal(t, want, got)
		})
	}
}
