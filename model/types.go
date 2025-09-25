package model

import (
	"time"

	"google.golang.org/grpc"
)

var Direction_value = map[string]int32{
	"FORWARD":  0,
	"BACKWARD": 1,
}

type SeriesResponse struct {
	Series []SeriesIdentifier
}

type SeriesIdentifier struct {
	Labels map[string]string
}

type LabelResponse struct {
	Values []string
}

type LabelRequest struct {
	Name   string
	Values bool
	Start  *time.Time
	End    *time.Time
	Query  string
}

type SampleQueryRequest struct {
	Selector string
	Start    time.Time
	End      time.Time
	Shards   []string
}

type SeriesRequest struct {
	Start  time.Time
	End    time.Time
	Groups []string
}

type Series struct {
	Labels  string
	Samples []Sample
}

type SampleQueryResponse struct {
	Series []Series
}

type Querier_QueryClient interface {
	Recv() (*QueryResponse, error)
	grpc.ClientStream
}

type QueryResponse struct {
	Streams []Stream
}

type Sample struct {
	Timestamp int64
	Value     float64
	Hash      uint64
}

type QueryRequest struct {
	Selector  string
	Limit     uint32
	Start     time.Time
	End       time.Time
	Direction Direction
	Shards    []string
}

type Direction int32

const (
	FORWARD  Direction = 0
	BACKWARD Direction = 1
)

// Stream contains a unique labels set as a string and a set of entries for it.
// We are not using the proto generated version but this custom one so that we
// can improve serialization see benchmark.
type Stream struct {
	Labels  string
	Entries []Entry
}

// Entry is a log entry with a timestamp.
type Entry struct {
	Timestamp time.Time
	Line      string
}

func (m *Stream) Equal(that any) bool {
	if that == nil {
		return m == nil
	}

	that1, ok := that.(*Stream)
	if !ok {
		that2, ok := that.(Stream)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return m == nil
	} else if m == nil {
		return false
	}
	if m.Labels != that1.Labels {
		return false
	}
	if len(m.Entries) != len(that1.Entries) {
		return false
	}
	for i := range m.Entries {
		if !m.Entries[i].Equal(that1.Entries[i]) {
			return false
		}
	}
	return true
}

func (m *Entry) Equal(that any) bool {
	if that == nil {
		return m == nil
	}

	that1, ok := that.(*Entry)
	if !ok {
		that2, ok := that.(Entry)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return m == nil
	} else if m == nil {
		return false
	}
	if !m.Timestamp.Equal(that1.Timestamp) {
		return false
	}
	if m.Line != that1.Line {
		return false
	}
	return true
}
