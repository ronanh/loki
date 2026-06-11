package logql

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/ronanh/loki/logql/log"
)

// The modern labels.Matcher carries a compiled FastRegexMatcher containing
// function fields, which never compare equal under reflect.DeepEqual. Like
// upstream Loki's syntax.RemoveFastRegexMatchers, the helpers below replace
// regex matchers by bare {Type, Name, Value} matchers (in place where
// possible) so ASTs and matcher slices can be compared structurally.

func removeFastRegexMatchers(matchers []*labels.Matcher) []*labels.Matcher {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		if m.Type == labels.MatchRegexp || m.Type == labels.MatchNotRegexp {
			m = &labels.Matcher{Type: m.Type, Name: m.Name, Value: m.Value}
		}
		result = append(result, m)
	}
	return result
}

func stripRegexFilterer(f log.LabelFilterer) log.LabelFilterer {
	switch v := f.(type) {
	case *log.BinaryLabelFilter:
		v.Left = stripRegexFilterer(v.Left)
		v.Right = stripRegexFilterer(v.Right)
	case *log.StringLabelFilter:
		if v.Matcher != nil &&
			(v.Type == labels.MatchRegexp || v.Type == labels.MatchNotRegexp) {
			return log.NewStringLabelFilter(
				&labels.Matcher{Type: v.Type, Name: v.Name, Value: v.Value},
			)
		}
	}
	return f
}

func stripRegexStage(s StageExpr) {
	switch v := s.(type) {
	case *labelFilterExpr:
		v.LabelFilterer = stripRegexFilterer(v.LabelFilterer)
	case *DropLabelsExpr:
		for i, d := range v.dropLabels {
			if d.Matcher != nil &&
				(d.Matcher.Type == labels.MatchRegexp || d.Matcher.Type == labels.MatchNotRegexp) {
				v.dropLabels[i].Matcher = &labels.Matcher{
					Type:  d.Matcher.Type,
					Name:  d.Matcher.Name,
					Value: d.Matcher.Value,
				}
			}
		}
	}
}

func stripRegexLogRange(r *logRange) {
	if r == nil {
		return
	}
	if r.left != nil {
		stripRegexExpr(r.left)
	}
	if r.unwrap != nil {
		for i, f := range r.unwrap.postFilters {
			r.unwrap.postFilters[i] = stripRegexFilterer(f)
		}
	}
}

// stripRegexExpr strips compiled regexes from an expression tree, in place,
// and returns it for convenience.
func stripRegexExpr(e Expr) Expr {
	switch v := e.(type) {
	case *matchersExpr:
		v.matchers = removeFastRegexMatchers(v.matchers)
	case *pipelineExpr:
		if v.left != nil {
			v.left.matchers = removeFastRegexMatchers(v.left.matchers)
		}
		for _, st := range v.pipeline {
			stripRegexStage(st)
		}
	case *rangeAggregationExpr:
		stripRegexLogRange(v.left)
	case *vectorAggregationExpr:
		stripRegexExpr(v.left)
	case *binOpExpr:
		stripRegexExpr(v.SampleExpr)
		stripRegexExpr(v.RHS)
	case *labelReplaceExpr:
		stripRegexExpr(v.left)
	}
	return e
}
