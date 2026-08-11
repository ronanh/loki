// Package labelhash defines the byte format this fork serializes label sets
// into before hashing, and nothing else.
//
// It exists to have exactly one definition of that format. The hashes derived
// from it are persisted by xlog — in stream definitions and in every chunk
// file — so the format is frozen: changing anything here re-identifies every
// stream ever stored. Two packages need it (logql for engine-side grouping,
// logql/log for the pipeline's stream and result hashes) and logql/log cannot
// import logql, since logql already imports logql/log. Hence a leaf package
// rather than one calling the other.
//
// The format is deliberately spelled out here instead of borrowed from
// prometheus. Upstream guarantees stability only for labels.StableHash; the
// helpers whose algorithm this matches (Labels.Hash, Labels.HashWithoutLabels)
// carry no such contract and are documented as free to change.
package labelhash

import "github.com/cespare/xxhash/v2"

// Sep separates a label name from its value, and one label from the next.
// Frozen: this is the byte prometheus uses in labels.StableHash.
const Sep = '\xff'

// Append appends one label in the frozen format: name, Sep, value, Sep.
func Append(b []byte, name, value string) []byte {
	b = append(b, name...)
	b = append(b, Sep)
	b = append(b, value...)
	b = append(b, Sep)
	return b
}

// Sum hashes a serialized label set. Frozen alongside the format itself: the
// choice of xxhash is as much a part of the persisted identity as Sep is.
func Sum(b []byte) uint64 {
	return xxhash.Sum64(b)
}
