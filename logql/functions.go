package logql

import (
	"fmt"
	"slices"

	"github.com/ronanh/loki/logql/log"
)

const unsupportedErr = "unsupported range vector aggregation operation: %s"

func (r rangeAggregationExpr) Extractor() (log.SampleExtractor, error) {
	return r.extractor(nil)
}

// extractor creates a SampleExtractor but allows for the grouping to be overridden.
func (r rangeAggregationExpr) extractor(override *grouping) (log.SampleExtractor, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	var groups []string
	var without bool
	var noLabels bool

	if r.grouping != nil {
		groups = r.grouping.groups
		without = r.grouping.without
		if len(groups) == 0 {
			noLabels = true
		}
	}

	// uses override if it exists
	if override != nil {
		groups = override.groups
		without = override.without
		if len(groups) == 0 {
			noLabels = true
		}
	}

	// absent_over_time cannot be grouped (yet?), so set noLabels=true
	// to make extraction more efficient and less likely to strip per query series limits.
	if r.operation == OpRangeTypeAbsent {
		noLabels = true
	}

	// a sorted copy: groups is the syntax tree's own slice, and the tree may be
	// kept and built into extractors again, possibly concurrently.
	groups = slices.Sorted(slices.Values(groups))

	var stages []log.Stage
	if p, ok := r.left.left.(*pipelineExpr); ok {
		// if the expression is a pipeline then take all stages into account first.
		st, err := p.pipeline.stages()
		if err != nil {
			return nil, err
		}
		stages = st
	}
	// unwrap...means we want to extract metrics from labels.
	if r.left.unwrap != nil {
		var convOp string
		switch r.left.unwrap.operation {
		case OpConvBytes:
			convOp = log.ConvertBytes
		case OpConvDuration, OpConvDurationSeconds:
			convOp = log.ConvertDuration
		default:
			convOp = log.ConvertFloat
		}

		return log.LabelExtractorWithStages(
			r.left.unwrap.identifier,
			convOp, groups, without, noLabels, stages,
			log.ReduceAndLabelFilter(r.left.unwrap.postFilters),
		)
	}
	// otherwise we extract metrics from the log line.
	switch r.operation {
	case OpRangeTypeRate, OpRangeTypeCount, OpRangeTypeAbsent:
		return log.NewLineSampleExtractor(log.CountExtractor, stages, groups, without, noLabels)
	case OpRangeTypeBytes, OpRangeTypeBytesRate:
		return log.NewLineSampleExtractor(log.BytesExtractor, stages, groups, without, noLabels)
	default:
		return nil, fmt.Errorf(unsupportedErr, r.operation)
	}
}
