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
	"fmt"
	"maps"
	"sort"
	"strings"
	"unicode/utf8"

	"regexp"
)

var (
	// NameValidationScheme determines the global default method of the name
	// validation to be used by all calls to IsValidMetricName() and LabelName
	// IsValid().
	//
	// Deprecated: This variable should not be used and might be removed in the
	// far future. If you wish to stick to the legacy name validation use
	// `IsValidLegacyMetricName()` and `LabelName.IsValidLegacy()` methods
	// instead. This variable is here as an escape hatch for emergency cases,
	// given the recent change from `LegacyValidation` to `UTF8Validation`, e.g.,
	// to delay UTF-8 migrations in time or aid in debugging unforeseen results of
	// the change. In such a case, a temporary assignment to `LegacyValidation`
	// value in the `init()` function in your main.go or so, could be considered.
	//
	// Historically we opted for a global variable for feature gating different
	// validation schemes in operations that were not otherwise easily adjustable
	// (e.g. Labels yaml unmarshaling). That could have been a mistake, a separate
	// Labels structure or package might have been a better choice. Given the
	// change was made and many upgraded the common already, we live this as-is
	// with this warning and learning for the future.
	NameValidationScheme = UTF8Validation
)

// ValidationScheme is a Go enum for determining how metric and label names will
// be validated by this library.
type ValidationScheme int

const (
	// UnsetValidation represents an undefined ValidationScheme.
	// Should not be used in practice.
	UnsetValidation ValidationScheme = iota

	// LegacyValidation is a setting that requires that all metric and label names
	// conform to the original Prometheus character requirements described by
	// MetricNameRE and LabelNameRE.
	LegacyValidation

	// UTF8Validation only requires that metric and label names be valid UTF-8
	// strings.
	UTF8Validation
)

// String returns the string representation of s.
func (s ValidationScheme) String() string {
	switch s {
	case UnsetValidation:
		return "unset"
	case LegacyValidation:
		return "legacy"
	case UTF8Validation:
		return "utf8"
	default:
		panic(fmt.Errorf("unhandled ValidationScheme: %d", s))
	}
}

// Set implements the pflag.Value interface.
func (s *ValidationScheme) Set(text string) error {
	switch text {
	case "":
		// Don't change the value.
	case LegacyValidation.String():
		*s = LegacyValidation
	case UTF8Validation.String():
		*s = UTF8Validation
	default:
		return fmt.Errorf("unrecognized ValidationScheme: %q", text)
	}
	return nil
}

// IsValidLabelName returns whether labelName is valid according to s.
func (s ValidationScheme) IsValidLabelName(labelName string) bool {
	switch s {
	case LegacyValidation:
		if len(labelName) == 0 {
			return false
		}
		for i, b := range labelName {
			// TODO: Apply De Morgan's law. Make sure there are tests for this.
			if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || (b >= '0' && b <= '9' && i > 0)) { //nolint:staticcheck
				return false
			}
		}
		return true
	case UTF8Validation:
		if len(labelName) == 0 {
			return false
		}
		return utf8.ValidString(labelName)
	default:
		panic(fmt.Sprintf("Invalid name validation scheme requested: %s", s))
	}
}

// Type implements the pflag.Value interface.
func (ValidationScheme) Type() string {
	return "validationScheme"
}

// MetricNameRE is a regular expression matching valid metric
// names. Note that the IsValidMetricName function performs the same
// check but faster than a match with this regular expression.
var MetricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// A Metric is similar to a LabelSet, but the key difference is that a Metric is
// a singleton and refers to one and only one stream of samples.
type Metric LabelSet

// Equal compares the metrics.
func (m Metric) Equal(o Metric) bool {
	return LabelSet(m).Equal(LabelSet(o))
}

// Before compares the metrics' underlying label sets.
func (m Metric) Before(o Metric) bool {
	return LabelSet(m).Before(LabelSet(o))
}

// Clone returns a copy of the Metric.
func (m Metric) Clone() Metric {
	clone := make(Metric, len(m))
	maps.Copy(clone, m)
	return clone
}

func (m Metric) String() string {
	metricName, hasName := m[MetricNameLabel]
	numLabels := len(m) - 1
	if !hasName {
		numLabels = len(m)
	}
	labelStrings := make([]string, 0, numLabels)
	for label, value := range m {
		if label != MetricNameLabel {
			labelStrings = append(labelStrings, fmt.Sprintf("%s=%q", label, value))
		}
	}

	switch numLabels {
	case 0:
		if hasName {
			return string(metricName)
		}
		return "{}"
	default:
		sort.Strings(labelStrings)
		return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ", "))
	}
}

// Fingerprint returns a Metric's Fingerprint.
func (m Metric) Fingerprint() Fingerprint {
	return LabelSet(m).Fingerprint()
}

// FastFingerprint returns a Metric's Fingerprint calculated by a faster hashing
// algorithm, which is, however, more susceptible to hash collisions.
func (m Metric) FastFingerprint() Fingerprint {
	return LabelSet(m).FastFingerprint()
}
