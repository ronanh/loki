package logproto

import (
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type wireMessage interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Size() int
}

var (
	wireLater   = time.Date(2024, 3, 5, 6, 7, 8, 123456789, time.UTC)
	wireEarlier = time.Date(1969, 7, 20, 20, 17, 40, 500, time.UTC) // negative seconds
)

// Servers and clients exchange these messages over gRPC, possibly running different versions during
// a rollout. The golden bytes are frozen: a diff means the wire format changed. Fix the code, never
// the golden.
// Messages that carry a Stream are left out: Stream.MarshalTo does not produce decodable bytes.
var wireCases = []struct {
	name   string
	msg    wireMessage
	golden string
}{
	{
		name: "QueryRequest",
		msg: &QueryRequest{
			Selector:  `{app="foo"} |= "bar"`,
			Limit:     100,
			Start:     wireEarlier,
			End:       wireLater,
			Direction: BACKWARD,
			Shards:    []string{"1_of_2"},
		},
		golden: "0a147b6170703d22666f6f227d207c3d20226261722210641a0e08e4ab9ef9ffffffffff0110f403220b088ced9aaf0610959aef3a28013a06315f6f665f32",
	},
	{
		name:   "QueryRequest with zero time",
		msg:    &QueryRequest{Selector: `{app="foo"}`, End: time.Time{}},
		golden: "0a0b7b6170703d22666f6f227d1a0b088092b8c398feffffff01220b088092b8c398feffffff01",
	},
	{
		name: "SampleQueryRequest",
		msg: &SampleQueryRequest{
			Selector: `rate({app="foo"}[1m])`,
			Start:    wireEarlier,
			End:      wireLater,
			Shards:   []string{"0_of_4"},
		},
		golden: "0a1572617465287b6170703d22666f6f227d5b316d5d29120e08e4ab9ef9ffffffffff0110f4031a0b088ced9aaf0610959aef3a2206305f6f665f34",
	},
	{
		name: "LabelRequest with times",
		msg: &LabelRequest{
			Name:   "app",
			Values: true,
			Start:  &wireEarlier,
			End:    &wireLater,
			Query:  `{env="prod"}`,
		},
		golden: "0a0361707010011a0e08e4ab9ef9ffffffffff0110f403220b088ced9aaf0610959aef3a2a0c7b656e763d2270726f64227d",
	},
	{
		name:   "LabelRequest without times",
		msg:    &LabelRequest{Name: "app"},
		golden: "0a03617070",
	},
	{
		name:   "LabelResponse",
		msg:    &LabelResponse{Values: []string{"bar", "foo"}},
		golden: "0a036261720a03666f6f",
	},
	{
		name: "StreamAdapter",
		msg: &StreamAdapter{
			Labels: `{app="foo", env="prod"}`,
			Entries: []EntryAdapter{
				{Timestamp: wireLater, Line: "hello"},
				{Timestamp: wireEarlier, Line: ""},
				{Timestamp: time.Unix(0, 0).UTC(), Line: "epoch"},
			},
		},
		golden: "0a177b6170703d22666f6f222c20656e763d2270726f64227d12140a0b088ced9aaf0610959aef3a120568656c6c6f12100a0e08e4ab9ef9ffffffffff0110f40312090a00120565706f6368",
	},
	{
		name: "SampleQueryResponse",
		msg: &SampleQueryResponse{Series: []Series{
			{
				Labels: `{app="foo"}`,
				Samples: []Sample{
					{Timestamp: 1709618828123, Value: 1.5, Hash: 42},
					{Timestamp: -1, Value: -2},
				},
			},
		}},
		golden: "0a370a0b7b6170703d22666f6f227d121208dba6e4e9e03111000000000000f83f182a121408ffffffffffffffffff011100000000000000c0",
	},
	{
		name:   "TailRequest",
		msg:    &TailRequest{Query: `{app="foo"}`, DelayFor: 2, Limit: 10, Start: wireLater},
		golden: "0a0b7b6170703d22666f6f227d1802200a2a0b088ced9aaf0610959aef3a",
	},
	{
		name: "TailResponse",
		msg: &TailResponse{
			DroppedStreams: []*DroppedStream{
				{From: wireEarlier, To: wireLater, Labels: `{app="foo"}`},
			},
		},
		golden: "122a0a0e08e4ab9ef9ffffffffff0110f403120b088ced9aaf0610959aef3a1a0b7b6170703d22666f6f227d",
	},
	{
		name:   "SeriesRequest",
		msg:    &SeriesRequest{Start: wireEarlier, End: wireLater, Groups: []string{`{app="foo"}`}},
		golden: "0a0e08e4ab9ef9ffffffffff0110f403120b088ced9aaf0610959aef3a1a0b7b6170703d22666f6f227d",
	},
	{
		// One label per series: map iteration order would make several labels non-deterministic.
		name: "SeriesResponse",
		msg: &SeriesResponse{
			Series: []SeriesIdentifier{{Labels: map[string]string{"app": "foo"}}},
		},
		golden: "0a0c0a0a0a036170701203666f6f",
	},
	{
		name:   "GetChunkIDsRequest",
		msg:    &GetChunkIDsRequest{Matchers: `{app="foo"}`, Start: wireEarlier, End: wireLater},
		golden: "0a0b7b6170703d22666f6f227d120e08e4ab9ef9ffffffffff0110f4031a0b088ced9aaf0610959aef3a",
	},
	{
		name:   "TailersCountResponse",
		msg:    &TailersCountResponse{Count: 3},
		golden: "0803",
	},
}

func TestWireFormatIsFrozen(t *testing.T) {
	for _, tc := range wireCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.msg.Marshal()
			require.NoError(t, err)
			if tc.golden == "" {
				t.Fatalf("missing golden: %x", data)
			}
			require.Equal(t, tc.golden, hex.EncodeToString(data))
			require.Equal(t, len(data), tc.msg.Size())

			decoded := reflect.New(reflect.TypeOf(tc.msg).Elem()).Interface().(wireMessage)
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, tc.msg, decoded)
		})
	}
}

// gRPC reaches these messages through protobuf-go's legacy wrapper, not through Marshal directly.
func TestWireFormatThroughGRPCCodec(t *testing.T) {
	codec := encoding.GetCodecV2(grpcproto.Name)
	for _, tc := range wireCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := codec.Marshal(tc.msg)
			require.NoError(t, err)
			require.Equal(t, tc.golden, hex.EncodeToString(data.Materialize()))

			decoded := reflect.New(reflect.TypeOf(tc.msg).Elem()).Interface()
			require.NoError(t, codec.Unmarshal(data, decoded))
			require.Equal(t, tc.msg, decoded)
		})
	}
}

func TestStdTimeMatchesTimestamppb(t *testing.T) {
	times := []time.Time{
		time.Time{}, // 0001-01-01, the earliest valid timestamp
		time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
		time.Unix(0, 0).UTC(),
		time.Unix(0, 1).UTC(),
		time.Unix(-1, 999999999).UTC(),
		wireEarlier,
		wireLater,
		time.Date(2024, 3, 5, 7, 7, 8, 1, time.FixedZone("CET", 3600)),
	}
	for _, ts := range times {
		want, err := proto.Marshal(timestamppb.New(ts))
		require.NoError(t, err)

		require.Equal(t, len(want), SizeOfStdTime(ts), ts)
		got := make([]byte, SizeOfStdTime(ts))
		n, err := StdTimeMarshalTo(ts, got)
		require.NoError(t, err)
		require.Equal(t, len(want), n)
		require.Equal(t, want, got, ts)

		var decoded time.Time
		require.NoError(t, StdTimeUnmarshal(&decoded, want))
		require.Equal(t, ts.UTC(), decoded)
	}
}

func TestStdTimeRejectsOutOfRange(t *testing.T) {
	for _, ts := range []time.Time{
		time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(0, 12, 31, 23, 59, 59, 0, time.UTC),
	} {
		require.Zero(t, SizeOfStdTime(ts), ts)
		_, err := StdTimeMarshalTo(ts, make([]byte, 32))
		require.Error(t, err, ts)

		data, err := proto.Marshal(timestamppb.New(ts))
		require.NoError(t, err)
		var decoded time.Time
		require.Error(t, StdTimeUnmarshal(&decoded, data), ts)
	}

	_, err := (&QueryRequest{End: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}).Marshal()
	require.Error(t, err)
}

func TestStdTimeUnmarshalRejectsBadInput(t *testing.T) {
	var decoded time.Time
	// nanos = 1e9
	require.Error(t, StdTimeUnmarshal(&decoded, []byte{0x10, 0x80, 0x94, 0xeb, 0xdc, 0x03}))
	// truncated varint
	require.Error(t, StdTimeUnmarshal(&decoded, []byte{0x08, 0x80}))
	// truncated tag
	require.Error(t, StdTimeUnmarshal(&decoded, []byte{0x80}))
}

func TestStdTimeUnmarshalSkipsUnknownFields(t *testing.T) {
	// seconds=1, unknown field 3 (bytes "ab"), nanos=2
	data := []byte{0x08, 0x01, 0x1a, 0x02, 'a', 'b', 0x10, 0x02}
	var decoded time.Time
	require.NoError(t, StdTimeUnmarshal(&decoded, data))
	require.Equal(t, time.Unix(1, 2).UTC(), decoded)
}
