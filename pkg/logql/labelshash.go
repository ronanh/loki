package logql

import (
	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	labelSep   = '\xfe'
	sep        = '\xff'
	MetricName = "__name__"
)

func HashLabels(b []byte, ls labels.Labels) (uint64, []byte) {
	b = b[:0]
	for _, v := range ls {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b), b
}

func HashWithoutLabels(b []byte, ls labels.Labels, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	for i := range ls {
		for j < len(names) && names[j] < ls[i].Name {
			j++
		}
		if j < len(names) && ls[i].Name == names[j] {
			continue
		}
		b = append(b, ls[i].Name...)
		b = append(b, sep)
		b = append(b, ls[i].Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b), b
}

func HashForLabels(b []byte, ls labels.Labels, names ...string) (uint64, []byte) {
	b = b[:0]
	i, j := 0, 0
	for i < len(ls) && j < len(names) {
		if names[j] == ls[i].Name {
			b = append(b, ls[i].Name...)
			b = append(b, sep)
			b = append(b, ls[i].Value...)
			b = append(b, sep)
			i++
			j++
		} else if ls[i].Name < names[j] {
			i++
		} else {
			j++
		}
	}
	return xxhash.Sum64(b), b
}
