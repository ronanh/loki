package logql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/prometheus/prometheus/promql"

	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/util"
)

// DownstreamSampleExpr is a SampleExpr which signals downstream computation
type DownstreamSampleExpr struct {
	shard *astmapper.ShardAnnotation
	SampleExpr
}

func (d DownstreamSampleExpr) String() string {
	return fmt.Sprintf("downstream<%s, shard=%s>", d.SampleExpr.String(), d.shard)
}

// DownstreamLogSelectorExpr is a LogSelectorExpr which signals downstream computation
type DownstreamLogSelectorExpr struct {
	shard *astmapper.ShardAnnotation
	LogSelectorExpr
}

func (d DownstreamLogSelectorExpr) String() string {
	return fmt.Sprintf("downstream<%s, shard=%s>", d.LogSelectorExpr.String(), d.shard)
}

// ConcatSampleExpr is an expr for concatenating multiple SampleExpr
// Contract: The embedded SampleExprs within a linked list of ConcatSampleExprs must be of the
// same structure. This makes special implementations of SampleExpr.Associative() unnecessary.
type ConcatSampleExpr struct {
	DownstreamSampleExpr
	next *ConcatSampleExpr
}

func (c ConcatSampleExpr) String() string {
	if c.next == nil {
		return c.DownstreamSampleExpr.String()
	}

	return fmt.Sprintf("%s ++ %s", c.DownstreamSampleExpr.String(), c.next.String())
}

// ConcatLogSelectorExpr is an expr for concatenating multiple LogSelectorExpr
type ConcatLogSelectorExpr struct {
	DownstreamLogSelectorExpr
	next *ConcatLogSelectorExpr
}

func (c ConcatLogSelectorExpr) String() string {
	if c.next == nil {
		return c.DownstreamLogSelectorExpr.String()
	}

	return fmt.Sprintf("%s ++ %s", c.DownstreamLogSelectorExpr.String(), c.next.String())
}

type Shards []astmapper.ShardAnnotation

func (xs Shards) Encode() (encoded []string) {
	for _, shard := range xs {
		encoded = append(encoded, shard.String())
	}

	return encoded
}

// ParseShards parses a list of string encoded shards
func ParseShards(strs []string) (Shards, error) {
	if len(strs) == 0 {
		return nil, nil
	}
	shards := make([]astmapper.ShardAnnotation, 0, len(strs))

	for _, str := range strs {
		shard, err := astmapper.ParseShard(str)
		if err != nil {
			return nil, err
		}
		shards = append(shards, shard)
	}
	return shards, nil
}

type Downstreamable interface {
	Downstreamer() Downstreamer
}

type DownstreamQuery struct {
	Expr   Expr
	Params Params
	Shards Shards
}

// Downstreamer is an interface for deferring responsibility for query execution.
// It is decoupled from but consumed by a downStreamEvaluator to dispatch ASTs.
type Downstreamer interface {
	Downstream(context.Context, []DownstreamQuery) ([]Result, error)
}

type errorQuerier struct{}

func (errorQuerier) SelectLogs(ctx context.Context, p SelectLogParams) (iter.EntryIterator, error) {
	return nil, errors.New("Unimplemented")
}

func (errorQuerier) SelectSamples(ctx context.Context, p SelectSampleParams) (iter.SampleIterator, error) {
	return nil, errors.New("Unimplemented")
}

// ConcatEvaluator joins multiple StepEvaluators.
// Contract: They must be of identical start, end, and step values.
func ConcatEvaluator(evaluators []StepEvaluator) (StepEvaluator, error) {
	return newStepEvaluator(
		func() (done bool, ts int64, vec promql.Vector) {
			var cur promql.Vector
			for _, eval := range evaluators {
				done, ts, cur = eval.Next()
				vec = append(vec, cur...)
			}
			return done, ts, vec
		},
		func() (lastErr error) {
			for _, eval := range evaluators {
				if err := eval.Close(); err != nil {
					lastErr = err
				}
			}
			return lastErr
		},
		func() error {
			var errs []error
			for _, eval := range evaluators {
				if err := eval.Error(); err != nil {
					errs = append(errs, err)
				}
			}
			switch len(errs) {
			case 0:
				return nil
			case 1:
				return errs[0]
			default:
				return util.MultiError(errs)
			}
		},
	)
}

// ResultStepEvaluator coerces a downstream vector or matrix into a StepEvaluator
func ResultStepEvaluator(res Result, params Params) (StepEvaluator, error) {
	var (
		start = params.Start()
		end   = params.End()
		step  = params.Step()
	)

	switch data := res.Data.(type) {
	case promql.Vector:
		var exhausted bool
		return newStepEvaluator(func() (bool, int64, promql.Vector) {
			if !exhausted {
				exhausted = true
				return true, start.UnixNano() / int64(time.Millisecond), data
			}
			return false, 0, nil
		}, nil, nil)
	case promql.Matrix:
		return NewMatrixStepper(start, end, step, data), nil
	default:
		return nil, fmt.Errorf("unexpected type (%s) uncoercible to StepEvaluator", data.Type())
	}
}

// ResultIterator coerces a downstream streams result into an iter.EntryIterator
func ResultIterator(res Result, params Params) (iter.EntryIterator, error) {
	streams, ok := res.Data.(Streams)
	if !ok {
		return nil, fmt.Errorf("unexpected type (%s) for ResultIterator; expected %s", res.Data.Type(), ValueTypeStreams)
	}
	return iter.NewStreamsIterator(context.Background(), streams, params.Direction()), nil
}
