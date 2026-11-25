package logql

import (
	"regexp"

	"github.com/prometheus/prometheus/model/labels"
)

// Read access to the syntax tree for a query engine outside this package.

type (
	LiteralExpr           = literalExpr
	RangeAggregationExpr  = rangeAggregationExpr
	VectorAggregationExpr = vectorAggregationExpr
	BinOpExpr             = binOpExpr
	LabelReplaceExpr      = labelReplaceExpr
)

func (e *literalExpr) Value() float64 { return e.value }

func (e *rangeAggregationExpr) Operation() string { return e.operation }

// Params returns the quantile of quantile_over_time, nil for the other operations.
func (e *rangeAggregationExpr) Params() *float64 { return e.params }

// HasUnwrap tells if the range aggregates the values of a label rather than the lines.
func (e *rangeAggregationExpr) HasUnwrap() bool { return e.left.unwrap != nil }

func (e *vectorAggregationExpr) Left() SampleExpr { return e.left }

func (e *vectorAggregationExpr) Operation() string { return e.operation }

// Params returns k for topk and bottomk.
func (e *vectorAggregationExpr) Params() int { return e.params }

func (e *binOpExpr) Op() string { return e.op }

func (e *binOpExpr) Opts() BinOpOptions { return e.opts }

func (e *labelReplaceExpr) Left() SampleExpr { return e.left }

func (e *labelReplaceExpr) Dst() string { return e.dst }

func (e *labelReplaceExpr) Src() string { return e.src }

func (e *labelReplaceExpr) Replacement() string { return e.replacement }

func (e *labelReplaceExpr) Regexp() *regexp.Regexp { return e.re }

// NewPipelineError returns the error reported for a sample whose labels carry a pipeline error.
func NewPipelineError(metric labels.Labels) error { return newPipelineErr(metric) }

// OptimizeSampleExpr simplifies expr before evaluation.
func OptimizeSampleExpr(expr SampleExpr) (SampleExpr, error) { return optimizeSampleExpr(expr) }
