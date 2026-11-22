package logql

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/logproto"
	"github.com/stretchr/testify/require"
)

func TestDefaultEvaluator_DivideByZero(t *testing.T) {
	var res promql.Sample
	mergeBinOp(OpTypeDiv,
		&promql.Sample{
			T: 1, F: 1,
		},
		&promql.Sample{
			T: 1, F: 0,
		},
		false,
		false,
		&res,
	)
	require.Equal(t, true, math.IsNaN(res.F))

	mergeBinOp(OpTypeMod,
		&promql.Sample{
			T: 1, F: 1,
		},
		&promql.Sample{
			T: 1, F: 0,
		},
		false,
		false,
		&res,
	)
	require.Equal(t, true, math.IsNaN(res.F))
}

func TestEvaluator_mergeBinOpComparisons(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		op       string
		lhs, rhs *promql.Sample
		expected *promql.Sample
	}{
		{
			`eq_0`,
			OpTypeCmpEQ,
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
		},
		{
			`eq_1`,
			OpTypeCmpEQ,
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 0,
			},
			&promql.Sample{
				F: 0,
			},
		},
		{
			`neq_0`,
			OpTypeNEQ,
			&promql.Sample{
				F: 0,
			},
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
		},
		{
			`neq_1`,
			OpTypeNEQ,
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 0,
			},
		},
		{
			`gt_0`,
			OpTypeGT,
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 0,
			},
		},
		{
			`gt_1`,
			OpTypeGT,
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 0,
			},
			&promql.Sample{
				F: 1,
			},
		},
		{
			`lt_0`,
			OpTypeLT,
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 0,
			},
		},
		{
			`lt_1`,
			OpTypeLT,
			&promql.Sample{
				F: 0,
			},
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
		},
		{
			`gte_0`,
			OpTypeGTE,
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 1,
			},
		},
		{
			`gt_1`,
			OpTypeGTE,
			&promql.Sample{
				F: 0,
			},
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 0,
			},
		},
		{
			`lte_0`,
			OpTypeLTE,
			&promql.Sample{
				F: 0,
			},
			&promql.Sample{
				F: 0,
			},
			&promql.Sample{
				F: 1,
			},
		},
		{
			`lte_1`,
			OpTypeLTE,
			&promql.Sample{
				F: 1,
			},
			&promql.Sample{
				F: 0,
			},
			&promql.Sample{
				F: 0,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// comparing a binop should yield the unfiltered (non-nil variant) regardless
			// of whether this is a vector-vector comparison or not.
			var res promql.Sample
			mergeBinOp(tc.op, tc.lhs, tc.rhs, false, false, &res)
			require.Equal(t, tc.expected, &res)
			mergeBinOp(tc.op, tc.lhs, tc.rhs, false, true, &res)
			require.Equal(t, tc.expected, &res)

			// vector-vector comparing when not filtering should propagate the labels for nil right
			// hand side matches,
			// but set the value to zero.
			mergeBinOp(tc.op, tc.lhs, nil, false, true, &res)
			require.Equal(
				t,
				&promql.Sample{
					F: 0,
				},
				&res,
			)

			//  test filtered variants
			if tc.expected.F == 0 {
				//  ensure zeroed predicates are filtered out
				merged := mergeBinOp(tc.op, tc.lhs, tc.rhs, true, false, &res)
				require.False(t, merged)
				merged = mergeBinOp(tc.op, tc.lhs, tc.rhs, true, true, &res)
				require.False(t, merged)

				// for vector-vector comparisons, ensure that nil right hand sides
				// translate into nil results
				merged = mergeBinOp(tc.op, tc.lhs, nil, true, true, &res)
				require.False(t, merged)

			}
		})
	}
}

func Test_MergeBinOpVectors_Filter(t *testing.T) {
	var res promql.Sample
	mergeBinOp(
		OpTypeGT,
		&promql.Sample{
			F: 2,
		},
		&promql.Sample{
			F: 0,
		},
		true,
		true,
		&res,
	)

	// ensure we return the left hand side's value (2) instead of the
	// comparison operator's result (1: the truthy answer)
	require.Equal(t, &promql.Sample{
		F: 2,
	}, &res)
}

func Test_absentLabels(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		expected labels.Labels
	}{
		{
			name:     "single equal matcher",
			query:    `absent_over_time({app="foo"}[5m])`,
			expected: labels.FromStrings("app", "foo"),
		},
		{
			name:     "single not-equal matcher",
			query:    `absent_over_time({app!="foo"}[5m])`,
			expected: labels.EmptyLabels(),
		},
		{
			name:     "two different keys with equal matchers",
			query:    `absent_over_time({app="foo", cluster="us-east"}[5m])`,
			expected: labels.FromStrings("app", "foo", "cluster", "us-east"),
		},
		{
			name:     "duplicate key: equal then not-equal",
			query:    `absent_over_time({app="foo", app!="bar"}[5m])`,
			expected: labels.FromStrings("app", "foo"),
		},
		{
			name:     "duplicate key: two not-equal matchers",
			query:    `absent_over_time({app!="foo", app!="bar"}[5m])`,
			expected: labels.EmptyLabels(),
		},
		{
			name:     "duplicate key: two equal matchers (contradictory)",
			query:    `absent_over_time({app="foo", app="bar"}[5m])`,
			expected: labels.EmptyLabels(),
		},
		{
			name:     "duplicate key: not-equal then equal",
			query:    `absent_over_time({app!="bar", app="foo"}[5m])`,
			expected: labels.FromStrings("app", "foo"),
		},
		{
			name:     "mixed: one duplicate key, one unique key",
			query:    `absent_over_time({app="foo", cluster="us-east", app!="bar"}[5m])`,
			expected: labels.FromStrings("app", "foo", "cluster", "us-east"),
		},
		{
			name:     "duplicate key: three equal matchers (contradictory)",
			query:    `absent_over_time({app="a", app="b", app="c"}[5m])`,
			expected: labels.EmptyLabels(),
		},
		{
			name:     "equal matcher with empty value",
			query:    `absent_over_time({app=""}[5m])`,
			expected: labels.EmptyLabels(),
		},
		{
			name:     "duplicate key: equal with empty value then not-equal",
			query:    `absent_over_time({app="", app!="bar"}[5m])`,
			expected: labels.EmptyLabels(),
		},
		{
			name:     "duplicate key: identical equal matchers (redundant)",
			query:    `absent_over_time({app="a", app="a"}[5m])`,
			expected: labels.FromStrings("app", "a"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := ParseSampleExpr(tc.query)
			require.NoError(t, err)
			got := absentLabels(expr)
			require.Equal(t, tc.expected, got)
		})
	}
}

type closeCountingSampleIterator struct {
	iter.SampleIterator
	closeCalls int
	err        error
}

func (it *closeCountingSampleIterator) Close() error {
	it.closeCalls++
	return nil
}

func (it *closeCountingSampleIterator) Error() error { return it.err }

func newRangeAggTestEvaluator(t *testing.T, query string, child iter.SampleIterator) StepEvaluator {
	t.Helper()
	expr, err := ParseSampleExpr(query)
	require.NoError(t, err)
	rangeExpr, ok := expr.(*rangeAggregationExpr)
	require.True(t, ok)
	start := time.Unix(0, 0)
	params := NewLiteralParams(
		query,
		start,
		start.Add(time.Minute),
		time.Second,
		0,
		logproto.FORWARD,
		0,
		nil,
	)
	evaluator, err := rangeAggEvaluator(iter.NewPeekingSampleIterator(child), rangeExpr, params)
	require.NoError(t, err)
	return evaluator
}

var rangeAggTestQueries = []string{
	`count_over_time({app="foo"}[1m])`,
	`absent_over_time({app="foo"}[1m])`,
}

func TestRangeAggEvaluatorCloseTwiceClosesChildOnce(t *testing.T) {
	for _, query := range rangeAggTestQueries {
		t.Run(query, func(t *testing.T) {
			child := &closeCountingSampleIterator{
				SampleIterator: iter.NewSeriesIterator(logproto.Series{
					Labels: `{app="foo"}`,
					Samples: []logproto.Sample{
						{Timestamp: time.Second.Nanoseconds(), Hash: 1, Value: 1},
					},
				}),
			}
			evaluator := newRangeAggTestEvaluator(t, query, child)
			next, _, _ := evaluator.Next()
			require.True(t, next)

			require.NoError(t, evaluator.Close())
			require.NoError(t, evaluator.Close())
			require.Equal(t, 1, child.closeCalls)
			require.NoError(t, evaluator.Error())
			next, _, _ = evaluator.Next()
			require.False(t, next)
		})
	}
}

func TestRangeAggEvaluatorKeepsErrorAfterClose(t *testing.T) {
	for _, query := range rangeAggTestQueries {
		t.Run(query, func(t *testing.T) {
			childErr := errors.New("child failed")
			child := &closeCountingSampleIterator{
				SampleIterator: iter.NewSeriesIterator(logproto.Series{Labels: `{app="foo"}`}),
				err:            childErr,
			}
			evaluator := newRangeAggTestEvaluator(t, query, child)
			require.ErrorIs(t, evaluator.Error(), childErr)

			require.NoError(t, evaluator.Close())
			require.ErrorIs(t, evaluator.Error(), childErr)
		})
	}
}

func TestVectorAggEvaluatorCloseTwiceClosesChildOnce(t *testing.T) {
	expr, err := ParseSampleExpr(`sum(count_over_time({app="foo"}[1m]))`)
	require.NoError(t, err)
	vectorExpr, ok := expr.(*vectorAggregationExpr)
	require.True(t, ok)

	var childCloseCalls int
	childEvaluator := SampleEvaluatorFunc(
		func(context.Context, SampleEvaluator, SampleExpr, Params) (StepEvaluator, error) {
			return newStepEvaluator(
				func() (bool, int64, promql.Vector) { return true, 0, promql.Vector{} },
				func() error { childCloseCalls++; return nil },
				nil,
			)
		},
	)
	start := time.Unix(0, 0)
	params := NewLiteralParams("", start, start, 0, 0, logproto.FORWARD, 0, nil)
	evaluator, err := vectorAggEvaluator(context.Background(), childEvaluator, vectorExpr, params)
	require.NoError(t, err)
	next, _, _ := evaluator.Next()
	require.True(t, next)

	require.NoError(t, evaluator.Close())
	require.NoError(t, evaluator.Close())
	require.Equal(t, 1, childCloseCalls)
	next, _, _ = evaluator.Next()
	require.False(t, next)
}

// closeCalls[n] counts Close calls on the nth evaluator built; building number failOnCall fails.
func stepEvaluatorWithCloseCounts(closeCalls []int, failOnCall int) SampleEvaluator {
	var calls int
	return SampleEvaluatorFunc(
		func(context.Context, SampleEvaluator, SampleExpr, Params) (StepEvaluator, error) {
			index := calls
			calls++
			if index == failOnCall {
				return nil, errors.New("cannot build evaluator")
			}
			return newStepEvaluator(
				func() (bool, int64, promql.Vector) {
					return true, 0, promql.Vector{{Metric: labels.FromStrings("app", "foo"), F: 1}}
				},
				func() error { closeCalls[index]++; return nil },
				nil,
			)
		},
	)
}

func parseBinOpExpr(t *testing.T) *binOpExpr {
	t.Helper()
	expr, err := ParseSampleExpr(
		`count_over_time({app="foo"}[1m]) / count_over_time({app="bar"}[1m])`,
	)
	require.NoError(t, err)
	binOp, ok := expr.(*binOpExpr)
	require.True(t, ok)
	return binOp
}

func TestBinOpStepEvaluatorCloseTwiceClosesChildrenOnce(t *testing.T) {
	closeCalls := make([]int, 2)
	start := time.Unix(0, 0)
	params := NewLiteralParams("", start, start, 0, 0, logproto.FORWARD, 0, nil)
	evaluator, err := newBinOpStepEvaluator(
		context.Background(),
		stepEvaluatorWithCloseCounts(closeCalls, -1),
		parseBinOpExpr(t),
		params,
	)
	require.NoError(t, err)
	next, _, _ := evaluator.Next()
	require.True(t, next)

	require.NoError(t, evaluator.Close())
	require.NoError(t, evaluator.Close())
	require.Equal(t, []int{1, 1}, closeCalls)
	require.NoError(t, evaluator.Error())
	next, _, _ = evaluator.Next()
	require.False(t, next)
}

func TestBinOpStepEvaluatorClosesLeftOperandWhenRightFails(t *testing.T) {
	closeCalls := make([]int, 2)
	start := time.Unix(0, 0)
	params := NewLiteralParams("", start, start, 0, 0, logproto.FORWARD, 0, nil)
	_, err := newBinOpStepEvaluator(
		context.Background(),
		stepEvaluatorWithCloseCounts(closeCalls, 1),
		parseBinOpExpr(t),
		params,
	)
	require.Error(t, err)
	require.Equal(t, []int{1, 0}, closeCalls)
}
