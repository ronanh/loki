package logql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

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
