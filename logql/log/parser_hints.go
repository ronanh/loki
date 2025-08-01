package log

import (
	"slices"
	"strings"
)

var noParserHints = &parserHint{}

// ParserHint are hints given to LogQL parsers.
// This is specially useful for parser that extract implicitly all possible label keys.
// This is used only within metric queries since it's rare that you need all label keys.
// For example in the following expression:
//
//	sum by (status_code) (rate({app="foo"} | json [5m]))
//
// All we need to extract is the status_code in the json parser.
type ParserHint interface {
	// Tells if a label with the given key should be extracted.
	ShouldExtract(key string) bool
	// Tells if there's any hint that start with the given prefix.
	// This allows to speed up key searching in nested structured like json.
	ShouldExtractPrefix(prefix string) bool
	// Tells if we should not extract any labels.
	// For example in :
	//		 sum(rate({app="foo"} | json [5m]))
	// We don't need to extract any labels from the log line.
	NoLabels() bool
}

type parserHint struct {
	noLabels       bool
	requiredLabels []string
}

func (p *parserHint) ShouldExtract(key string) bool {
	if len(p.requiredLabels) == 0 {
		return true
	}
	return slices.Contains(p.requiredLabels, key)
}

func (p *parserHint) ShouldExtractPrefix(prefix string) bool {
	if len(p.requiredLabels) == 0 {
		return true
	}
	for _, l := range p.requiredLabels {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}

	return false
}

func (p *parserHint) NoLabels() bool {
	return p.noLabels
}

// newParserHint creates a new parser hint using the list of labels that are seen and required in a
// query.
func newParserHint(
	requiredLabelNames, groups []string,
	without, noLabels bool,
	metricLabelName string,
) *parserHint {
	// If a parsed label collides with a stream label we add the `_extracted` suffix to it, however
	// hints are used by the parsers before we know they will collide with a stream label and hence
	// before the _extracted suffix is added. Therefore we must strip the _extracted suffix from any
	// required labels
	// that were parsed from somewhere in the query, say in a filter or an aggregation clause.
	// Because it's possible for a valid json or logfmt key to already end with _extracted, we'll
	// just leave the existing entry ending with _extracted but also add a version with the suffix
	// removed.
	var nbExtracted int
	for _, l := range requiredLabelNames {
		if strings.HasSuffix(l, "_extracted") {
			nbExtracted++
		}
	}
	nbAddLabels := len(groups)
	if metricLabelName != "" {
		nbAddLabels++
	}
	if noLabels {
		if len(requiredLabelNames)+nbAddLabels+nbExtracted == 0 {
			return &parserHint{noLabels: true}
		}
	} else if without || len(groups) == 0 {
		return noParserHints
	}

	res := make([]string, 0, len(requiredLabelNames)+nbAddLabels+nbExtracted)
	res = append(res, requiredLabelNames...)
	for _, l := range requiredLabelNames {
		if strings.HasSuffix(l, "_extracted") {
			res = append(res, strings.TrimSuffix(l, "_extracted"))
		}
	}
	res = append(res, groups...)
	if metricLabelName != "" {
		res = append(res, metricLabelName)
	}
	res = uniqueString(res)
	return &parserHint{requiredLabels: res}
}
