package logql

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

var foldOps = []string{
	OpTypeAdd, OpTypeSub, OpTypeMul, OpTypeDiv, OpTypeMod, OpTypePow,
	OpTypeCmpEQ, OpTypeNEQ, OpTypeGT, OpTypeGTE, OpTypeLT, OpTypeLTE,
}

func TestFoldLiterals(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  float64
	}{
		{`1 + 2`, 3},
		{`1 - 2`, -1},
		{`2 * 3`, 6},
		{`3 / 2`, 1.5},
		{`3 / 0`, math.NaN()},
		{`7 % 3`, 1},
		{`7 % 0`, math.NaN()},
		{`2 ^ 10`, 1024},
		{`1 == 1`, 1},
		{`1 != 1`, 0},
		{`2 > 1`, 1},
		{`1 >= 2`, 0},
		{`1 < 2`, 1},
		{`2 <= 1`, 0},
		{`2 > bool 1`, 1},
		{`(1 + 2) * 3`, 9},
	} {
		t.Run(tc.query, func(t *testing.T) {
			expr, err := ParseSampleExpr(tc.query)
			require.NoError(t, err)
			lit, ok := expr.(*LiteralExpr)
			require.True(t, ok, "%T", expr)
			if math.IsNaN(tc.want) {
				require.True(t, math.IsNaN(lit.Value()), "got %v", lit.Value())
				return
			}
			require.Equal(t, tc.want, lit.Value())
		})
	}
}

func TestFoldLiteralsMatchesMergeBinOp(t *testing.T) {
	values := []float64{-3, -1, -0.5, 0, 0.5, 1, 2, 3, math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, op := range foldOps {
		for _, left := range values {
			for _, right := range values {
				var want promql.Sample
				mergeBinOp(
					op,
					&promql.Sample{F: left},
					&promql.Sample{F: right},
					false,
					false,
					&want,
				)
				got := foldLiterals(op, left, right)
				if math.IsNaN(want.F) {
					require.True(t, math.IsNaN(got), "%v %s %v: got %v", left, op, right, got)
					continue
				}
				require.Equal(t, want.F, got, "%v %s %v", left, op, right)
			}
		}
	}
}
