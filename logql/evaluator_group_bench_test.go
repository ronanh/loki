package logql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/ronanh/loki/logproto"
)

// The shapes below mirror what the xlog alerter actually feeds the engine
// (measured on supprol4, 2026-08-01):
//
//   - stream label sets: 23..29 labels, ~700..970 bytes once packed by the
//     stringlabels representation (median 25 labels / 827 bytes),
//   - ~60..120 input series per step reaching the vector aggregation,
//   - ~20 distinct groups per query,
//   - 2..4 steps per query, then the whole evaluator is torn down (one query
//     per Kafka message).
//
// benchStreamLabels builds `n` such label sets. `groupCard` distinct values are
// used for the grouped labels so the group count is controllable independently
// of the series count.
func benchStreamLabels(n, groupCard int) []labels.Labels {
	out := make([]labels.Labels, n)
	for i := range out {
		g := i % groupCard
		out[i] = labels.FromStrings(
			"admin_tenant", "AdmLogCentralizationsupprol3eu2",
			"cluster", "supprol3",
			"datacenter", "eu2",
			"emitter_admin_tenant", "AdmObsCollectorocfarmeu2",
			"environment", "DEVENV",
			"full_cluster", "supprol3_main",
			"k8s_container_name", fmt.Sprintf("alertgw%d", g),
			"k8s_pod_name", fmt.Sprintf("ak8fdtgqi7xmlspbebzn-alertgw-6d6d9c5bb7-vb%03d", g),
			"k8s_workload_def_name", "alertgw",
			"k8s_workload_name", "ak8fdtgqi7xmlspbebzn-alertgw",
			"logtype", "STDOUT",
			"path", fmt.Sprintf("alertgw%d.log", i),
			"path_hash", fmt.Sprintf("1AF1DF7857734B7AFBE15201891257%02d", i%100),
			"region", "eu2",
			"resource_id", fmt.Sprintf("ak8fdtgqi7xmlspbebzn-alertgw-6d6d9c5bb7-v%03d", i),
			"resource_ip", fmt.Sprintf("10.42.48.%d", i%256),
			"resource_level", "resource",
			"resource_name", fmt.Sprintf("alertgw_6d6d9c5bb7-v%03d-alertgw", i),
			"service_id", "LogCentralization_wTnZbEbPSLmX7iQGtdF8KA",
			"service_type", "Container",
			"svc_def_name", "LogCentralization",
			"svc_def_version", "1.11.0_20260730213939916",
			"svc_workload_definition_name", "alertgw",
			"svc_workload_definition_version", "1.11.0",
			"svc_workload_name", "ak8fdtgqi7xmlspbebzn-alertgw",
		)
	}
	return out
}

// benchStepEvaluator replays the same vector `steps` times, the way the range
// vector iterator re-emits the same window series at every step (the
// labels.Labels values are the identical ones, as rangeVectorIterator keeps
// them in its `metrics` slice for the lifetime of the query).
type benchStepEvaluator struct {
	vec   promql.Vector
	steps int
	i     int
}

func (b *benchStepEvaluator) Next() (bool, int64, promql.Vector) {
	if b.i >= b.steps {
		return false, 0, nil
	}
	b.i++
	return true, int64(b.i) * 30000, b.vec
}

func (b *benchStepEvaluator) Close() error { b.i = 0; return nil }
func (b *benchStepEvaluator) Error() error { return nil }

func benchVectorAggParams() Params {
	return NewLiteralParams(
		"",
		time.Unix(0, 0),
		time.Unix(120, 0),
		30*time.Second,
		0,
		logproto.FORWARD,
		0,
		nil,
	)
}

// benchVectorAgg drives vectorAggEvaluator over a whole query lifecycle
// (build -> N steps -> close), which is what one alert evaluation does.
func benchVectorAgg(b *testing.B, nSeries, groupCard, steps int, groups []string, without bool) {
	b.Helper()

	lbls := benchStreamLabels(nSeries, groupCard)
	vec := make(promql.Vector, nSeries)
	for i := range vec {
		vec[i] = promql.Sample{T: 30000, F: float64(i), Metric: lbls[i]}
	}

	expr := &vectorAggregationExpr{
		left:      nil,
		grouping:  &grouping{groups: append([]string(nil), groups...), without: without},
		operation: OpTypeSum,
	}
	ev := SampleEvaluatorFunc(
		func(_ context.Context, _ SampleEvaluator, _ SampleExpr, _ Params) (StepEvaluator, error) {
			return &benchStepEvaluator{vec: vec, steps: steps}, nil
		},
	)
	q := benchVectorAggParams()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		se, err := vectorAggEvaluator(ctx, ev, expr, q)
		if err != nil {
			b.Fatal(err)
		}
		for {
			ok, _, out := se.Next()
			if !ok {
				break
			}
			if len(out) != groupCard {
				b.Fatalf("expected %d groups, got %d", groupCard, len(out))
			}
		}
		if err := se.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// Production shape: 100 series -> 20 groups, 3 steps, sum by (k8s_pod_name).
func BenchmarkVectorAggProdShape(b *testing.B) {
	benchVectorAgg(b, 100, 20, 3, []string{"k8s_pod_name"}, false)
}

// Same, but grouping on two labels that are far apart in the sorted label set.
func BenchmarkVectorAggProdShapeTwoGroups(b *testing.B) {
	benchVectorAgg(b, 100, 20, 3, []string{"k8s_container_name", "resource_level"}, false)
}

// Sensitivity to the step count: 1 step is the worst case for any across-step
// memo (nothing to reuse), 8 steps the best.
func BenchmarkVectorAggSteps(b *testing.B) {
	for _, steps := range []int{1, 2, 3, 4, 8} {
		b.Run(fmt.Sprintf("steps=%d", steps), func(b *testing.B) {
			benchVectorAgg(b, 100, 20, steps, []string{"k8s_pod_name"}, false)
		})
	}
}

// Sensitivity to the input fan-out.
func BenchmarkVectorAggSeries(b *testing.B) {
	for _, n := range []int{20, 60, 100, 400} {
		b.Run(fmt.Sprintf("series=%d", n), func(b *testing.B) {
			benchVectorAgg(b, n, 20, 3, []string{"k8s_pod_name"}, false)
		})
	}
}

// `without` uses the other hashing helper and rebuilds a much wider group label
// set, so it is measured separately.
func BenchmarkVectorAggWithout(b *testing.B) {
	benchVectorAgg(
		b,
		100,
		20,
		3,
		[]string{"path", "path_hash", "resource_id", "resource_ip", "resource_name"},
		true,
	)
}

// Isolated cost of the hashing helpers on a production-sized label set.
func BenchmarkHashHelpers(b *testing.B) {
	ls := benchStreamLabels(1, 1)[0]
	names := []string{"k8s_pod_name"}
	buf := make([]byte, 0, 1024)

	b.Run("HashForLabels", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, buf = HashForLabels(buf, ls, names...)
		}
	})
	b.Run("HashLabels", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, buf = HashLabels(buf, ls)
		}
	})
	b.Run("Labels.Hash", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = ls.Hash()
		}
	})
	b.Run("LabelsEqual", func(b *testing.B) {
		other := ls
		b.ReportAllocs()
		for range b.N {
			if ls != other {
				b.Fatal("unexpected")
			}
		}
	})
}
