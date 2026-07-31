package logql

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/ronanh/loki/logql/labelhash"
)

// MetricName is the reserved label name carrying a metric's name.
const MetricName = model.MetricNameLabel

// HashLabels and HashWithoutLabels serialize a label set with
// labelhash.Append and hash it with labelhash.Sum; they differ only in which
// labels they include. HashForLabels delegates to prometheus instead (see its
// own doc comment) but produces the identical frozen byte format.
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

// HashForLabels hashes the labels of ls whose name is in names, which must be
// sorted in ascending order.
//
// It delegates to labels.Labels.HashForLabels, which produces byte-for-byte the
// same buffer (see TestHashForLabelsMatchesPromImpl) but walks the packed label
// string directly: no closure call per label, and it stops decoding as soon as
// names is exhausted instead of decoding the whole label set. On the label sets
// the alerter groups (25 labels, ~830 bytes packed) that is 2.4x faster for a
// `by (k8s_pod_name)` and 6.6x for a grouping label that sorts early.
//
// Unlike HashLabels and HashWithoutLabels, this depends on prometheus's own
// hash algorithm rather than the frozen labelhash format directly — it is
// only safe because the two are pinned byte-identical by
// TestHashForLabelsMatchesPromImpl/Randomized, which must keep passing across
// every future prometheus bump.
func HashForLabels(b []byte, ls labels.Labels, names ...string) (uint64, []byte) {
	return ls.HashForLabels(b, names...)
}
