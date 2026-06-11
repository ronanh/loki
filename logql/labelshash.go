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

func HashForLabels(b []byte, ls labels.Labels, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	ls.Range(func(l labels.Label) {
		for j < len(names) && names[j] < l.Name {
			j++
		}
		if j < len(names) && l.Name == names[j] {
			b = append(b, l.Name...)
			b = append(b, sep)
			b = append(b, l.Value...)
			b = append(b, sep)
			j++
		}
	})
	return xxhash.Sum64(b), b
}
