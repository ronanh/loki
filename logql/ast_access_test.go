package logql

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestASTAccess(t *testing.T) {
	expr, err := ParseSampleExpr(
		`topk(3, sum by (app) (quantile_over_time(0.9, {app="a"} | unwrap latency [5m])))`)
	require.NoError(t, err)
	top, ok := expr.(*VectorAggregationExpr)
	require.True(t, ok, "%T", expr)
	require.Equal(t, OpTypeTopK, top.Operation())
	require.Equal(t, 3, top.Params())

	sum, ok := top.Left().(*VectorAggregationExpr)
	require.True(t, ok, "%T", top.Left())
	require.Equal(t, OpTypeSum, sum.Operation())
	require.Equal(t, []string{"app"}, sum.Groups())
	require.False(t, sum.Without())

	quantile, ok := sum.Left().(*RangeAggregationExpr)
	require.True(t, ok, "%T", sum.Left())
	require.Equal(t, OpRangeTypeQuantile, quantile.Operation())
	require.Equal(t, 0.9, *quantile.Params())
	require.True(t, quantile.HasUnwrap())
	require.Equal(t, 5*time.Minute, quantile.Interval())

	expr, err = ParseSampleExpr(`rate({app="a"}[1m]) > bool 2`)
	require.NoError(t, err)
	binOp, ok := expr.(*BinOpExpr)
	require.True(t, ok, "%T", expr)
	require.Equal(t, OpTypeGT, binOp.Op())
	require.True(t, binOp.Opts().ReturnBool)
	rate, ok := binOp.SampleExpr.(*RangeAggregationExpr)
	require.True(t, ok, "%T", binOp.SampleExpr)
	require.Nil(t, rate.Params())
	require.False(t, rate.HasUnwrap())
	require.Equal(t, 2.0, binOp.RHS.(*LiteralExpr).Value())

	expr, err = ParseSampleExpr(
		`label_replace(rate({app="a"}[1m]), "dst", "$1", "src", "(.*)-x")`)
	require.NoError(t, err)
	replace, ok := expr.(*LabelReplaceExpr)
	require.True(t, ok, "%T", expr)
	require.Equal(t, "dst", replace.Dst())
	require.Equal(t, "src", replace.Src())
	require.Equal(t, "$1", replace.Replacement())
	require.True(t, replace.Regexp().MatchString("a-x"))
	_, ok = replace.Left().(*RangeAggregationExpr)
	require.True(t, ok, "%T", replace.Left())
}

func TestNewPipelineError(t *testing.T) {
	err := NewPipelineError(labels.FromStrings("app", "a", "__error__", "JSONParserErr"))
	require.ErrorContains(t, err, "JSONParserErr")
}

func TestOptimizeSampleExpr(t *testing.T) {
	expr, err := ParseSampleExpr(`sum(rate({app="a"} | line_format "{{.app}}" [1m]))`)
	require.NoError(t, err)
	optimized, err := OptimizeSampleExpr(expr)
	require.NoError(t, err)
	require.Equal(t, `sum(rate({app="a"}[1m]))`, optimized.String())
}
