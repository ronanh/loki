package logql

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

// hashForLabelsRange is the implementation HashForLabels had before it was
// delegated to labels.Labels.HashForLabels: a Range with a closure, walking the
// whole label set. It is kept here as the reference the delegation is pinned
// against.
func hashForLabelsRange(b []byte, ls labels.Labels, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	ls.Range(func(l labels.Label) {
		for j < len(names) && names[j] < l.Name {
			j++
		}
		if j < len(names) && l.Name == names[j] {
			b = append(b, l.Name...)
			b = append(b, sep)
			b = append(b, l.Value...)
			b = append(b, sep)
			j++
		}
	})
	return xxhash.Sum64(b), b
}

// TestHashForLabelsMatchesPromImpl pins that delegating HashForLabels to the
// prometheus implementation is byte-for-byte equivalent to the Range-based one,
// including its early exit once names is exhausted. Both require names sorted
// ascending, which vectorAggEvaluator guarantees.
func TestHashForLabelsMatchesPromImpl(t *testing.T) {
	cases := []struct {
		name  string
		lbls  labels.Labels
		names []string
	}{
		{"empty labels", labels.EmptyLabels(), []string{"a"}},
		{"empty names", labels.FromStrings("a", "1", "b", "2"), nil},
		{"both empty", labels.EmptyLabels(), nil},
		{"all matched", labels.FromStrings("a", "1", "b", "2"), []string{"a", "b"}},
		{"none matched", labels.FromStrings("a", "1", "b", "2"), []string{"c", "d"}},
		{"first only", labels.FromStrings("a", "1", "b", "2", "c", "3"), []string{"a"}},
		{"last only", labels.FromStrings("a", "1", "b", "2", "c", "3"), []string{"c"}},
		{"name before all labels", labels.FromStrings("m", "1"), []string{"a"}},
		{"name after all labels", labels.FromStrings("a", "1"), []string{"z"}},
		{"interleaved misses", labels.FromStrings("a", "1", "c", "3", "e", "5"),
			[]string{"b", "c", "d", "e", "f"}},
		{"duplicate names", labels.FromStrings("a", "1", "b", "2"), []string{"a", "a"}},
		{"empty value", labels.FromStrings("a", "", "b", "2"), []string{"a"}},
		{"sep inside value", labels.FromStrings("a", "x\xffy", "b", "2"), []string{"a", "b"}},
		{"long value", labels.FromStrings("a", string(make([]byte, 300)), "b", "2"),
			[]string{"a", "b"}},
		{"metric name", labels.FromStrings("__name__", "m", "a", "1"),
			[]string{"__name__", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantHash, wantBuf := hashForLabelsRange(nil, tc.lbls, tc.names...)
			gotHash, gotBuf := HashForLabels(nil, tc.lbls, tc.names...)
			require.Equal(t, string(wantBuf), string(gotBuf))
			require.Equal(t, wantHash, gotHash)
		})
	}
}

func TestHashForLabelsMatchesPromImplRandomized(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	pool := []string{"a", "aa", "b", "c", "cc", "d", "e", "f", "g", "h", "zz"}

	var refBuf, gotBuf []byte
	for iter := range 5000 {
		nl := rnd.Intn(len(pool) + 1)
		lnames := append([]string(nil), pool...)
		rnd.Shuffle(len(lnames), func(i, j int) { lnames[i], lnames[j] = lnames[j], lnames[i] })
		lnames = lnames[:nl]
		slices.Sort(lnames)

		pairs := make([]string, 0, 2*len(lnames))
		for i, n := range lnames {
			pairs = append(pairs, n, fmt.Sprintf("v%d", i))
		}
		ls := labels.FromStrings(pairs...)

		nn := rnd.Intn(len(pool) + 1)
		names := append([]string(nil), pool...)
		rnd.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
		names = names[:nn]
		slices.Sort(names)

		var refHash, gotHash uint64
		refHash, refBuf = hashForLabelsRange(refBuf, ls, names...)
		gotHash, gotBuf = HashForLabels(gotBuf, ls, names...)
		require.Equal(t, string(refBuf), string(gotBuf), "iter %d ls=%v names=%v", iter, ls, names)
		require.Equal(t, refHash, gotHash, "iter %d", iter)
	}
}

// BenchmarkHashForLabelsImpl compares the two implementations on a production
// sized label set, for grouping labels at different positions in the sorted set
// (the prometheus implementation stops decoding once names is exhausted, so the
// win depends on where the grouping labels sort).
func BenchmarkHashForLabelsImpl(b *testing.B) {
	ls := benchStreamLabels(1, 1)[0]
	for _, names := range [][]string{
		{"admin_tenant"},      // sorts first
		{"k8s_pod_name"},      // middle
		{"svc_workload_name"}, // sorts last
		{"k8s_container_name", "resource_level"},
	} {
		buf := make([]byte, 0, 1024)
		b.Run("range/"+names[0], func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, buf = hashForLabelsRange(buf, ls, names...)
			}
		})
		b.Run("prom/"+names[0], func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, buf = HashForLabels(buf, ls, names...)
			}
		})
	}
}
