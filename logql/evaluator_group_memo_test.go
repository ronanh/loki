package logql

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

// scriptedStepEvaluator replays a fixed script of step vectors, so a test can
// control exactly how the input series set changes from one step to the next.
type scriptedStepEvaluator struct {
	steps []promql.Vector
	i     int
}

func (s *scriptedStepEvaluator) Next() (bool, int64, promql.Vector) {
	if s.i >= len(s.steps) {
		return false, 0, nil
	}
	v := s.steps[s.i]
	s.i++
	return true, int64(s.i) * 30000, v
}

func (s *scriptedStepEvaluator) Close() error { return nil }
func (s *scriptedStepEvaluator) Error() error { return nil }

// runSumBy evaluates `sum by (groups) (...)` / `sum without (groups) (...)` over
// the scripted steps and returns, per step, the aggregated value per group label
// set (keyed by its string form).
func runSumBy(
	t *testing.T,
	steps []promql.Vector,
	groups []string,
	without bool,
) []map[string]float64 {
	t.Helper()

	expr := &vectorAggregationExpr{
		grouping:  &grouping{groups: append([]string(nil), groups...), without: without},
		operation: OpTypeSum,
	}
	ev := SampleEvaluatorFunc(
		func(_ context.Context, _ SampleEvaluator, _ SampleExpr, _ Params) (StepEvaluator, error) {
			return &scriptedStepEvaluator{steps: steps}, nil
		},
	)

	se, err := vectorAggEvaluator(context.Background(), ev, expr, benchVectorAggParams())
	require.NoError(t, err)

	var got []map[string]float64
	for {
		ok, _, out := se.Next()
		if !ok {
			break
		}
		step := make(map[string]float64, len(out))
		for _, s := range out {
			key := s.Metric.String()
			_, dup := step[key]
			require.False(t, dup, "group %s emitted twice in one step", key)
			step[key] = s.F
		}
		got = append(got, step)
	}
	require.NoError(t, se.Close())
	return got
}

// wantSumBy is the reference implementation: no memo, no grouping cache, just
// build the group label set per sample and add up.
func wantSumBy(steps []promql.Vector, groups []string, without bool) []map[string]float64 {
	out := make([]map[string]float64, 0, len(steps))
	for _, vec := range steps {
		step := map[string]float64{}
		for _, s := range vec {
			var kept []labels.Label
			s.Metric.Range(func(l labels.Label) {
				if slices.Contains(groups, l.Name) != without {
					kept = append(kept, l)
				}
			})
			step[labels.New(kept...).String()] += s.F
		}
		out = append(out, step)
	}
	return out
}

func sampleVec(lbls []labels.Labels, idx []int, val func(i int) float64) promql.Vector {
	vec := make(promql.Vector, 0, len(idx))
	for _, i := range idx {
		vec = append(vec, promql.Sample{T: 30000, F: val(i), Metric: lbls[i]})
	}
	return vec
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// TestVectorAggGroupMemo_VectorChanges pins that the per-series memo stays
// correct when the step vector is not a verbatim replay: series drop out, come
// back, get reordered or appended. Every one of those makes the position-keyed
// memo entries misalign, which must only cost a recomputation.
func TestVectorAggGroupMemo_VectorChanges(t *testing.T) {
	lbls := benchStreamLabels(24, 6)
	val := func(i int) float64 { return float64(i + 1) }

	shuffled := seq(24)
	rnd := rand.New(rand.NewSource(1))
	rnd.Shuffle(
		len(shuffled),
		func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] },
	)

	for _, tc := range []struct {
		name  string
		steps [][]int
	}{
		{"identical", [][]int{seq(24), seq(24), seq(24)}},
		{"series drops out", [][]int{seq(24), {0, 1, 2, 5, 6, 7, 8}, seq(24)}},
		{"series appended", [][]int{{0, 1, 2}, {0, 1, 2, 3, 4, 5}, seq(24)}},
		{"reordered", [][]int{seq(24), shuffled, seq(24)}},
		{"shrinks then grows", [][]int{seq(24), {3}, seq(24)}},
		{"empty step in the middle", [][]int{seq(24), {}, seq(24)}},
		{"single series", [][]int{{7}, {7}, {7}}},
	} {
		for _, g := range []struct {
			name    string
			groups  []string
			without bool
		}{
			{"by one label", []string{"k8s_pod_name"}, false},
			{"by two labels", []string{"k8s_container_name", "resource_level"}, false},
			{"by absent label", []string{"not_a_label"}, false},
			{"without", []string{"path", "path_hash", "resource_id", "resource_ip", "resource_name"}, true},
		} {
			t.Run(tc.name+"/"+g.name, func(t *testing.T) {
				steps := make([]promql.Vector, 0, len(tc.steps))
				for _, idx := range tc.steps {
					steps = append(steps, sampleVec(lbls, idx, val))
				}
				want := wantSumBy(steps, g.groups, g.without)
				got := runSumBy(t, steps, g.groups, g.without)
				require.Equal(t, want, got)
			})
		}
	}
}

// TestVectorAggGroupMemo_NotReusedAcrossQueries is the load-bearing invariant:
// evaluatorGroups is pooled, and a memo entry is only valid for the grouping
// clause it was computed under. Running back-to-back queries over the *same*
// label sets with different grouping clauses reuses the pooled evaluatorGroups,
// so a memo that survived Close would silently return the previous query's
// groups.
func TestVectorAggGroupMemo_NotReusedAcrossQueries(t *testing.T) {
	lbls := benchStreamLabels(64, 8)
	val := func(i int) float64 { return float64(i + 1) }
	steps := []promql.Vector{
		sampleVec(lbls, seq(64), val),
		sampleVec(lbls, seq(64), val),
	}

	clauses := []struct {
		groups  []string
		without bool
	}{
		{[]string{"k8s_pod_name"}, false},
		{[]string{"k8s_container_name"}, false},
		{[]string{"resource_level"}, false},
		{[]string{"path"}, false},
		{[]string{"path", "path_hash", "resource_id", "resource_ip", "resource_name"}, true},
		{[]string{"k8s_pod_name"}, false},
	}

	// two passes so every clause runs after every other one
	for pass := range 2 {
		for i, c := range clauses {
			t.Run(fmt.Sprintf("pass%d/clause%d", pass, i), func(t *testing.T) {
				require.Equal(t,
					wantSumBy(steps, c.groups, c.without),
					runSumBy(t, steps, c.groups, c.without),
				)
			})
		}
	}
}

// TestVectorAggGroupMemo_Randomized fuzzes the step vectors against the
// reference implementation.
func TestVectorAggGroupMemo_Randomized(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	lbls := benchStreamLabels(80, 9)
	groupings := [][]string{
		{"k8s_pod_name"},
		{"k8s_container_name", "resource_level"},
		{"cluster", "environment", "k8s_pod_name"},
		{"not_a_label"},
		nil,
	}

	for iter := range 300 {
		nSteps := 1 + rnd.Intn(6)
		steps := make([]promql.Vector, 0, nSteps)
		for range nSteps {
			n := rnd.Intn(len(lbls) + 1)
			idx := rnd.Perm(len(lbls))[:n]
			steps = append(steps, sampleVec(lbls, idx, func(i int) float64 {
				return float64(i%7) - 3
			}))
		}
		g := groupings[rnd.Intn(len(groupings))]
		without := rnd.Intn(2) == 0
		if len(g) == 0 && without {
			// `without ()` over an empty list is `sum()`, still valid; keep it.
			_ = without
		}
		want := wantSumBy(steps, g, without)
		got := runSumBy(t, steps, g, without)
		require.Equal(t, want, got, "iteration %d groups=%v without=%v", iter, g, without)
	}
}

// TestVectorAggGroupMemo_WideFanOutIsBounded pins that the memo does not grow
// past maxGroupMemoEntries: the tail of a very wide step vector is simply not
// memoized, and the result stays correct.
func TestVectorAggGroupMemo_WideFanOutIsBounded(t *testing.T) {
	n := maxGroupMemoEntries + 137
	lbls := benchStreamLabels(n, 11)
	steps := []promql.Vector{
		sampleVec(lbls, seq(n), func(i int) float64 { return float64(i) }),
		sampleVec(lbls, seq(n), func(i int) float64 { return float64(i) }),
	}
	groups := []string{"k8s_pod_name"}

	require.Equal(t, wantSumBy(steps, groups, false), runSumBy(t, steps, groups, false))

	eg := evaluatorGroupsPool.Get().(*evaluatorGroups)
	defer evaluatorGroupsPool.Put(eg)
	eg.resizeMemo(n)
	require.LessOrEqual(t, len(eg.memo), maxGroupMemoEntries)
}

// TestVectorAggGroupMemo_ClearedOnClose pins the mechanism the invariant above
// relies on.
func TestVectorAggGroupMemo_ClearedOnClose(t *testing.T) {
	lbls := benchStreamLabels(8, 4)
	steps := []promql.Vector{sampleVec(lbls, seq(8), func(int) float64 { return 1 })}
	_ = runSumBy(t, steps, []string{"k8s_pod_name"}, false)

	eg := evaluatorGroupsPool.Get().(*evaluatorGroups)
	defer evaluatorGroupsPool.Put(eg)
	require.Empty(t, eg.memo)
	require.Empty(t, eg.groups)
	require.False(t, eg.sorted)
	for _, m := range eg.memo[:cap(eg.memo)] {
		require.Equal(t, groupMemo{}, m)
	}
}
