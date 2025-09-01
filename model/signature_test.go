// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricToFingerprint(t *testing.T) {
	scenarios := []struct {
		in  LabelSet
		out Fingerprint
	}{
		{
			in:  LabelSet{},
			out: 14695981039346656037,
		},
		{
			in:  LabelSet{"name": "garland, briggs", "fear": "love is not enough"},
			out: 5799056148416392346,
		},
	}

	for i, scenario := range scenarios {
		actual := labelSetToFingerprint(scenario.in)

		if actual != scenario.out {
			t.Errorf("%d. expected %d, got %d", i, scenario.out, actual)
		}
	}
}

func TestMetricToFastFingerprint(t *testing.T) {
	scenarios := []struct {
		in  LabelSet
		out Fingerprint
	}{
		{
			in:  LabelSet{},
			out: 14695981039346656037,
		},
		{
			in:  LabelSet{"name": "garland, briggs", "fear": "love is not enough"},
			out: 12952432476264840823,
		},
	}

	for i, scenario := range scenarios {
		actual := labelSetToFastFingerprint(scenario.in)

		if actual != scenario.out {
			t.Errorf("%d. expected %d, got %d", i, scenario.out, actual)
		}
	}
}

func benchmarkMetricToFingerprint(b *testing.B, ls LabelSet, e Fingerprint) {
	for i := 0; i < b.N; i++ {
		a := labelSetToFingerprint(ls)
		require.Equalf(b, a, e, "expected signature of %d for %s, got %d", e, ls, a)
	}
}

func BenchmarkMetricToFingerprintScalar(b *testing.B) {
	benchmarkMetricToFingerprint(b, nil, 14695981039346656037)
}

func BenchmarkMetricToFingerprintSingle(b *testing.B) {
	benchmarkMetricToFingerprint(
		b,
		LabelSet{"first-label": "first-label-value"},
		5146282821936882169,
	)
}

func BenchmarkMetricToFingerprintDouble(b *testing.B) {
	benchmarkMetricToFingerprint(
		b,
		LabelSet{"first-label": "first-label-value", "second-label": "second-label-value"},
		3195800080984914717,
	)
}

func BenchmarkMetricToFingerprintTriple(b *testing.B) {
	benchmarkMetricToFingerprint(
		b,
		LabelSet{
			"first-label":  "first-label-value",
			"second-label": "second-label-value",
			"third-label":  "third-label-value",
		},
		13843036195897128121,
	)
}

func benchmarkMetricToFastFingerprint(b *testing.B, ls LabelSet, e Fingerprint) {
	for i := 0; i < b.N; i++ {
		a := labelSetToFastFingerprint(ls)
		require.Equalf(b, a, e, "expected signature of %d for %s, got %d", e, ls, a)
	}
}

func BenchmarkMetricToFastFingerprintScalar(b *testing.B) {
	benchmarkMetricToFastFingerprint(b, nil, 14695981039346656037)
}

func BenchmarkMetricToFastFingerprintSingle(b *testing.B) {
	benchmarkMetricToFastFingerprint(
		b,
		LabelSet{"first-label": "first-label-value"},
		5147259542624943964,
	)
}

func BenchmarkMetricToFastFingerprintDouble(b *testing.B) {
	benchmarkMetricToFastFingerprint(
		b,
		LabelSet{"first-label": "first-label-value", "second-label": "second-label-value"},
		18269973311206963528,
	)
}

func BenchmarkMetricToFastFingerprintTriple(b *testing.B) {
	benchmarkMetricToFastFingerprint(
		b,
		LabelSet{
			"first-label":  "first-label-value",
			"second-label": "second-label-value",
			"third-label":  "third-label-value",
		},
		15738406913934009676,
	)
}

func benchmarkMetricToFastFingerprintConc(b *testing.B, ls LabelSet, e Fingerprint, concLevel int) {
	var start, end sync.WaitGroup
	start.Add(1)
	end.Add(concLevel)
	errc := make(chan error, 1)

	for range concLevel {
		go func() {
			start.Wait()
			for j := b.N / concLevel; j >= 0; j-- {
				if a := labelSetToFastFingerprint(ls); a != e {
					select {
					case errc <- fmt.Errorf("expected signature of %d for %s, got %d", e, ls, a):
					default:
					}
				}
			}
			end.Done()
		}()
	}
	b.ResetTimer()
	start.Done()
	end.Wait()

	select {
	case err := <-errc:
		b.Fatal(err)
	default:
	}
}

func BenchmarkMetricToFastFingerprintTripleConc1(b *testing.B) {
	benchmarkMetricToFastFingerprintConc(
		b,
		LabelSet{
			"first-label":  "first-label-value",
			"second-label": "second-label-value",
			"third-label":  "third-label-value",
		},
		15738406913934009676,
		1,
	)
}

func BenchmarkMetricToFastFingerprintTripleConc2(b *testing.B) {
	benchmarkMetricToFastFingerprintConc(
		b,
		LabelSet{
			"first-label":  "first-label-value",
			"second-label": "second-label-value",
			"third-label":  "third-label-value",
		},
		15738406913934009676,
		2,
	)
}

func BenchmarkMetricToFastFingerprintTripleConc4(b *testing.B) {
	benchmarkMetricToFastFingerprintConc(
		b,
		LabelSet{
			"first-label":  "first-label-value",
			"second-label": "second-label-value",
			"third-label":  "third-label-value",
		},
		15738406913934009676,
		4,
	)
}

func BenchmarkMetricToFastFingerprintTripleConc8(b *testing.B) {
	benchmarkMetricToFastFingerprintConc(
		b,
		LabelSet{
			"first-label":  "first-label-value",
			"second-label": "second-label-value",
			"third-label":  "third-label-value",
		},
		15738406913934009676,
		8,
	)
}
