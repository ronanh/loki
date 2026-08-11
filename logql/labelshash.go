package logql

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/ronanh/loki/logql/labelhash"
)

// MetricName is the reserved label name carrying a metric's name.
const MetricName = model.MetricNameLabel

// The three functions below serialize a label set with labelhash.Append and
// hash it with labelhash.Sum; they differ only in which labels they include.
//
// Note for anyone comparing them with the hashes in logql/log: none of these
// skips MetricName. HashWithoutLabels drops exactly the names it is given and
// nothing else, unlike the identically-named labels.Labels.HashWithoutLabels
// and unlike the pipeline's own stream hash, both of which also drop
// MetricName. Sample metrics reaching the engine carry no MetricName, so the
// distinction does not change any value produced here.

// HashLabels hashes every label in ls.
func HashLabels(b []byte, ls labels.Labels) (uint64, []byte) {
	b = b[:0]
	ls.Range(func(l labels.Label) {
		b = labelhash.Append(b, l.Name, l.Value)
	})
	return labelhash.Sum(b), b
}

// HashWithoutLabels hashes every label in ls except those in names.
// 'names' have to be sorted in ascending order.
func HashWithoutLabels(b []byte, ls labels.Labels, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	ls.Range(func(l labels.Label) {
		for j < len(names) && names[j] < l.Name {
			j++
		}
		if j < len(names) && l.Name == names[j] {
			return
		}
		b = labelhash.Append(b, l.Name, l.Value)
	})
	return labelhash.Sum(b), b
}

// HashForLabels hashes only the labels in ls whose name is in names.
// 'names' have to be sorted in ascending order.
func HashForLabels(b []byte, ls labels.Labels, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	ls.Range(func(l labels.Label) {
		for j < len(names) && names[j] < l.Name {
			j++
		}
		if j < len(names) && l.Name == names[j] {
			b = labelhash.Append(b, l.Name, l.Value)
			j++
		}
	})
	return labelhash.Sum(b), b
}
