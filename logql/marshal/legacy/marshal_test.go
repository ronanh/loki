package marshal

import (
	"log"
	"testing"
	"time"

	json "github.com/json-iterator/go"
	loghttp "github.com/ronanh/loki/loghttp/legacy"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql"
	"github.com/stretchr/testify/require"
)

// covers responses from /api/prom/query
var queryTests = []struct {
	actual   logql.Streams
	expected string
}{
	{
		logql.Streams{
			logproto.Stream{
				Entries: []logproto.Entry{
					{
						Timestamp: mustParse(time.RFC3339Nano, "2019-09-13T18:32:22.380001319Z"),
						Line:      "super line",
					},
				},
				Labels: `{test="test"}`,
			},
		},
		`{
			"streams":[
				{
					"labels":"{test=\"test\"}",
					"entries":[
						{
							"ts": "2019-09-13T18:32:22.380001319Z",
							"line": "super line"
						}
					]
				}
			],
			"stats" : {
				"ingester" : {
					"compressedBytes": 0,
					"decompressedBytes": 0,
					"decompressedLines": 0,
					"headChunkBytes": 0,
					"headChunkLines": 0,
					"totalBatches": 0,
					"totalChunksMatched": 0,
					"totalDuplicates": 0,
					"totalLinesSent": 0,
					"totalReached": 0
				},
				"store": {
					"compressedBytes": 0,
					"decompressedBytes": 0,
					"decompressedLines": 0,
					"headChunkBytes": 0,
					"headChunkLines": 0,
					"chunksDownloadTime": 0,
					"totalChunksRef": 0,
					"totalChunksDownloaded": 0,
					"totalDuplicates": 0
				},
				"summary": {
					"bytesProcessedPerSecond": 0,
					"execTime": 0,
					"linesProcessedPerSecond": 0,
					"totalBytesProcessed":0,
					"totalLinesProcessed":0
				}
			}
		}`,
	},
}

// covers responses from /api/prom/tail and /api/prom/tail
var tailTests = []struct {
	actual   loghttp.TailResponse
	expected string
}{
	{
		loghttp.TailResponse{
			Streams: []logproto.Stream{
				{
					Entries: []logproto.Entry{
						{
							Timestamp: mustParse(
								time.RFC3339Nano,
								"2019-09-13T18:32:22.380001319Z",
							),
							Line: "super line",
						},
					},
					Labels: "{test=\"test\"}",
				},
			},
			DroppedEntries: []loghttp.DroppedEntry{
				{
					Timestamp: mustParse(time.RFC3339Nano, "2019-09-13T18:32:22.380001319Z"),
					Labels:    "{test=\"test\"}",
				},
			},
		},
		`{
			"streams": [
				{
					"labels": "{test=\"test\"}",
					"entries": [
						{
							"ts": "2019-09-13T18:32:22.380001319Z",
							"line": "super line"
						}
					]
				}
			],
			"dropped_entries": [
				{
					"Timestamp": "2019-09-13T18:32:22.380001319Z",
					"Labels": "{test=\"test\"}"
				}
			]
		}`,
	},
}

func Test_MarshalTailResponse(t *testing.T) {
	for i, tailTest := range tailTests {
		// marshal model object
		bytes, err := json.Marshal(tailTest.actual)
		require.NoError(t, err)

		testJSONBytesEqual(t, []byte(tailTest.expected), bytes, "Tail Test %d failed", i)
	}
}

func Test_QueryResponseMarshalLoop(t *testing.T) {
	for i, queryTest := range queryTests {
		var r map[string]any

		err := json.Unmarshal([]byte(queryTest.expected), &r)
		require.NoError(t, err)

		jsonOut, err := json.Marshal(r)
		require.NoError(t, err)

		testJSONBytesEqual(
			t,
			[]byte(queryTest.expected),
			jsonOut,
			"Query Marshal Loop %d failed",
			i,
		)
	}
}

func Test_TailResponseMarshalLoop(t *testing.T) {
	for i, tailTest := range tailTests {
		var r loghttp.TailResponse

		err := json.Unmarshal([]byte(tailTest.expected), &r)
		require.NoError(t, err)

		jsonOut, err := json.Marshal(r)
		require.NoError(t, err)

		testJSONBytesEqual(t, []byte(tailTest.expected), jsonOut, "Tail Marshal Loop %d failed", i)
	}
}

func testJSONBytesEqual(
	t *testing.T,
	expected []byte,
	actual []byte,
	msg string,
	args ...any,
) {
	var expectedValue map[string]any
	err := json.Unmarshal(expected, &expectedValue)
	require.NoError(t, err)

	var actualValue map[string]any
	err = json.Unmarshal(actual, &actualValue)
	require.NoError(t, err)

	require.Equalf(t, expectedValue, actualValue, msg, args)
}

func mustParse(l string, t string) time.Time {
	ret, err := time.Parse(l, t)
	if err != nil {
		log.Fatalf("Failed to parse %s", t)
	}

	return ret
}
