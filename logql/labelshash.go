package logql

import (
	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	sep        = '\xff'
	MetricName = "__name__"
)

func HashLabels(b []byte, ls labels.Labels) (uint64, []byte) {
	b = b[:0]
	ls.Range(func(l labels.Label) {
		b = append(b, l.Name...)
		b = append(b, sep)
		b = append(b, l.Value...)
		b = append(b, sep)
	})
	return xxhash.Sum64(b), b
}

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
		b = append(b, l.Name...)
		b = append(b, sep)
		b = append(b, l.Value...)
		b = append(b, sep)
	})
	return xxhash.Sum64(b), b
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
func HashForLabels(b []byte, ls labels.Labels, names ...string) (uint64, []byte) {
	return ls.HashForLabels(b, names...)
}
