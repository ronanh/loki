// Copyright 2013 The Prometheus Authors
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
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func testMetric(t testing.TB) {
	scenarios := []struct {
		input           LabelSet
		fingerprint     Fingerprint
		fastFingerprint Fingerprint
	}{
		{
			input:           LabelSet{},
			fingerprint:     14695981039346656037,
			fastFingerprint: 14695981039346656037,
		},
		{
			input: LabelSet{
				"first_name":   "electro",
				"occupation":   "robot",
				"manufacturer": "westinghouse",
			},
			fingerprint:     5911716720268894962,
			fastFingerprint: 11310079640881077873,
		},
		{
			input: LabelSet{
				"x": "y",
			},
			fingerprint:     8241431561484471700,
			fastFingerprint: 13948396922932177635,
		},
		{
			input: LabelSet{
				"a": "bb",
				"b": "c",
			},
			fingerprint:     3016285359649981711,
			fastFingerprint: 3198632812309449502,
		},
		{
			input: LabelSet{
				"a":  "b",
				"bb": "c",
			},
			fingerprint:     7122421792099404749,
			fastFingerprint: 5774953389407657638,
		},
	}

	for i, scenario := range scenarios {
		input := Metric(scenario.input)

		if scenario.fingerprint != input.Fingerprint() {
			t.Errorf("%d. expected %d, got %d", i, scenario.fingerprint, input.Fingerprint())
		}
		if scenario.fastFingerprint != input.FastFingerprint() {
			t.Errorf(
				"%d. expected %d, got %d",
				i,
				scenario.fastFingerprint,
				input.FastFingerprint(),
			)
		}
	}
}

func TestMetric(t *testing.T) {
	testMetric(t)
}

func BenchmarkMetric(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testMetric(b)
	}
}

func TestValidationScheme(t *testing.T) {
	var scheme ValidationScheme
	require.Equal(t, UnsetValidation, scheme)
}

func TestValidationScheme_String(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scheme ValidationScheme
		want   string
	}{
		{
			name:   "Unset",
			scheme: UnsetValidation,
			want:   "unset",
		},
		{
			name:   "Legacy",
			scheme: LegacyValidation,
			want:   "legacy",
		},
		{
			name:   "UTF8",
			scheme: UTF8Validation,
			want:   "utf8",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.scheme.String())
		})
	}
}

func TestMetricClone(t *testing.T) {
	m := Metric{
		"first_name":   "electro",
		"occupation":   "robot",
		"manufacturer": "westinghouse",
	}

	m2 := m.Clone()

	if len(m) != len(m2) {
		t.Errorf("expected the length of the cloned metric to be equal to the input metric")
	}

	for ln, lv := range m2 {
		expected := m[ln]
		if expected != lv {
			t.Errorf("expected label value %s but got %s for label name %s", expected, lv, ln)
		}
	}
}

func TestMetricToString(t *testing.T) {
	scenarios := []struct {
		name     string
		input    Metric
		expected string
	}{
		{
			name: "valid metric without __name__ label",
			input: Metric{
				"first_name":   "electro",
				"occupation":   "robot",
				"manufacturer": "westinghouse",
			},
			expected: `{first_name="electro", manufacturer="westinghouse", occupation="robot"}`,
		},
		{
			name: "valid metric with __name__ label",
			input: Metric{
				"__name__":     "electro",
				"occupation":   "robot",
				"manufacturer": "westinghouse",
			},
			expected: `electro{manufacturer="westinghouse", occupation="robot"}`,
		},
		{
			name: "empty metric with __name__ label",
			input: Metric{
				"__name__": "fooname",
			},
			expected: "fooname",
		},
		{
			name:     "empty metric",
			input:    Metric{},
			expected: "{}",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			actual := scenario.input.String()
			if actual != scenario.expected {
				t.Errorf("expected string output %s but got %s", scenario.expected, actual)
			}
		})
	}
}

// TestProtoFormatUnchanged checks to see if the proto format changed, in which
// case EscapeMetricFamily will need to be updated.
func TestProtoFormatUnchanged(t *testing.T) {
	scenarios := []struct {
		name         string
		input        proto.Message
		expectFields []string
	}{
		{
			name:         "MetricFamily",
			input:        &dto.MetricFamily{},
			expectFields: []string{"name", "help", "type", "metric", "unit"},
		},
		{
			name:  "Metric",
			input: &dto.Metric{},
			expectFields: []string{
				"label",
				"gauge",
				"counter",
				"summary",
				"untyped",
				"histogram",
				"timestamp_ms",
			},
		},
		{
			name:         "LabelPair",
			input:        &dto.LabelPair{},
			expectFields: []string{"name", "value"},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			desc := scenario.input.ProtoReflect().Descriptor()
			fields := desc.Fields()
			if fields.Len() != len(scenario.expectFields) {
				t.Errorf(
					"dto.MetricFamily changed length, expected %d, got %d",
					len(scenario.expectFields),
					fields.Len(),
				)
			}

			for i := 0; i < fields.Len(); i++ {
				got := fields.Get(i).TextName()
				if got != scenario.expectFields[i] {
					t.Errorf(
						"dto.MetricFamily field mismatch, expected %s got %s",
						scenario.expectFields[i],
						got,
					)
				}
			}
		})
	}
}
