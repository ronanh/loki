package logproto

import (
	"errors"
	"io"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// Encoding of a time.Time as a google.protobuf.Timestamp message, for the stdtime fields.

const (
	// Seconds of 0001-01-01T00:00:00Z, the earliest valid Timestamp.
	minValidSeconds = -62135596800
	// Seconds of 10000-01-01T00:00:00Z, just after the latest valid Timestamp.
	maxValidSeconds = 253402300800

	timestampSecondsField protowire.Number = 1
	timestampNanosField   protowire.Number = 2
)

func validateTimestamp(seconds int64, nanos int32) error {
	switch {
	case seconds < minValidSeconds || seconds >= maxValidSeconds:
		return errors.New("timestamp: seconds out of range: " + strconv.FormatInt(seconds, 10))
	case nanos < 0 || nanos >= 1e9:
		return errors.New("timestamp: nanos out of range: " + strconv.Itoa(int(nanos)))
	}
	return nil
}

func timestampSize(seconds int64, nanos int32) int {
	n := 0
	if seconds != 0 {
		n += protowire.SizeTag(timestampSecondsField) + protowire.SizeVarint(uint64(seconds))
	}
	if nanos != 0 {
		n += protowire.SizeTag(timestampNanosField) + protowire.SizeVarint(uint64(nanos))
	}
	return n
}

// SizeOfStdTime returns the encoded size of t, or 0 if t cannot be encoded.
func SizeOfStdTime(t time.Time) int {
	seconds, nanos := t.Unix(), int32(t.Nanosecond())
	if validateTimestamp(seconds, nanos) != nil {
		return 0
	}
	return timestampSize(seconds, nanos)
}

// StdTimeMarshalTo encodes t at the start of data and returns the number of bytes written.
func StdTimeMarshalTo(t time.Time, data []byte) (int, error) {
	seconds, nanos := t.Unix(), int32(t.Nanosecond())
	if err := validateTimestamp(seconds, nanos); err != nil {
		return 0, err
	}
	size := timestampSize(seconds, nanos)
	if len(data) < size {
		return 0, io.ErrShortBuffer
	}
	b := data[:0]
	if seconds != 0 {
		b = protowire.AppendTag(b, timestampSecondsField, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(seconds))
	}
	if nanos != 0 {
		b = protowire.AppendTag(b, timestampNanosField, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(nanos))
	}
	return len(b), nil
}

// StdTimeUnmarshal decodes data into t, in UTC.
func StdTimeUnmarshal(t *time.Time, data []byte) error {
	var seconds int64
	var nanos int32
	for len(data) > 0 {
		field, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		if wireType == protowire.VarintType &&
			(field == timestampSecondsField || field == timestampNanosField) {
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			if field == timestampSecondsField {
				seconds = int64(v)
			} else {
				nanos = int32(v)
			}
			continue
		}
		n = protowire.ConsumeFieldValue(field, wireType, data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
	}
	if err := validateTimestamp(seconds, nanos); err != nil {
		return err
	}
	*t = time.Unix(seconds, int64(nanos)).UTC()
	return nil
}
