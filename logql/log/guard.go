//go:build slicelabels || dedupelabels

package log

// This fork requires the stringlabels representation of labels.Labels
// (comparable values, zero-alloc Hash, opaque immutable data). Building with
// the slicelabels or dedupelabels build tags is unsupported: fail the build
// with a self-describing compile error.
var _ = THIS_FORK_REQUIRES_THE_STRINGLABELS_LABELS_REPRESENTATION
