package iter

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSize      = 10
	defaultLabels = "{foo=\"baz\"}"
)

func TestIterator(t *testing.T) {
	for i, tc := range []struct {
		iterator  EntryIterator
		generator generator
		length    int64
		labels    string
	}{
		// Test basic identity.
		{
			iterator:  mkStreamIterator(identity, defaultLabels),
			generator: identity,
			length:    testSize,
			labels:    defaultLabels,
		},

		// Test basic identity (backwards).
		{
			iterator:  mkStreamIterator(inverse(identity), defaultLabels),
			generator: inverse(identity),
			length:    testSize,
			labels:    defaultLabels,
		},

		// Test dedupe of overlapping iterators with the heap iterator.
		{
			iterator: NewHeapIterator(context.Background(), []EntryIterator{
				mkStreamIterator(offset(0, identity), defaultLabels),
				mkStreamIterator(offset(testSize/2, identity), defaultLabels),
				mkStreamIterator(offset(testSize, identity), defaultLabels),
			}, logproto.FORWARD),
			generator: identity,
			length:    2 * testSize,
			labels:    defaultLabels,
		},

		// Test dedupe of overlapping iterators with the heap iterator (backward).
		{
			iterator: NewHeapIterator(context.Background(), []EntryIterator{
				mkStreamIterator(inverse(offset(0, identity)), defaultLabels),
				mkStreamIterator(inverse(offset(-testSize/2, identity)), defaultLabels),
				mkStreamIterator(inverse(offset(-testSize, identity)), defaultLabels),
			}, logproto.BACKWARD),
			generator: inverse(identity),
			length:    2 * testSize,
			labels:    defaultLabels,
		},

		// Test dedupe of entries with the same timestamp but different entries.
		// Disable: not relevant anymore
		// {
		// 	iterator: NewHeapIterator(context.Background(), []EntryIterator{
		// 		mkStreamIterator(offset(0, constant(0)), defaultLabels),
		// 		mkStreamIterator(offset(0, constant(0)), defaultLabels),
		// 		mkStreamIterator(offset(testSize, constant(0)), defaultLabels),
		// 	}, logproto.FORWARD),
		// 	generator: constant(0),
		// 	length:    2 * testSize,
		// 	labels:    defaultLabels,
		// },

		// Test basic identity with non-default labels.
		{
			iterator:  mkStreamIterator(identity, "{foobar: \"bazbar\"}"),
			generator: identity,
			length:    testSize,
			labels:    "{foobar: \"bazbar\"}",
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			for i := range int64(tc.length) {
				assert.True(t, tc.iterator.Next())
				assert.Equal(t, tc.generator(i), tc.iterator.Entry(), fmt.Sprintln("iteration", i))
				assert.Equal(t, tc.labels, tc.iterator.Labels(), fmt.Sprintln("iteration", i))
			}

			assert.False(t, tc.iterator.Next())
			assert.NoError(t, tc.iterator.Error())
			assert.NoError(t, tc.iterator.Close())
		})
	}
}

func TestIteratorMultipleLabels(t *testing.T) {
	for i, tc := range []struct {
		iterator  EntryIterator
		generator generator
		length    int64
		labels    func(int64) string
	}{
		// Test merging with differing labels but same timestamps and values.
		{
			iterator: NewHeapIterator(context.Background(), []EntryIterator{
				mkStreamIterator(identity, "{foobar: \"baz1\"}"),
				mkStreamIterator(identity, "{foobar: \"baz2\"}"),
			}, logproto.FORWARD),
			generator: func(i int64) logproto.Entry {
				return identity(i / 2)
			},
			length: testSize * 2,
			labels: func(i int64) string {
				if i%2 == 0 {
					return "{foobar: \"baz1\"}"
				}
				return "{foobar: \"baz2\"}"
			},
		},

		// Test merging with differing labels but all the same timestamps and different values.
		{
			iterator: NewHeapIterator(context.Background(), []EntryIterator{
				mkStreamIterator(constant(0), "{foobar: \"baz1\"}"),
				mkStreamIterator(constant(0), "{foobar: \"baz2\"}"),
			}, logproto.FORWARD),
			generator: func(i int64) logproto.Entry {
				return constant(0)(i % testSize)
			},
			length: testSize * 2,
			labels: func(i int64) string {
				if i/testSize == 0 {
					return "{foobar: \"baz1\"}"
				}
				return "{foobar: \"baz2\"}"
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			for i := range int64(tc.length) {
				assert.True(t, tc.iterator.Next())
				assert.Equal(t, tc.generator(i), tc.iterator.Entry(), fmt.Sprintln("iteration", i))
				assert.Equal(t, tc.labels(i), tc.iterator.Labels(), fmt.Sprintln("iteration", i))
			}

			assert.False(t, tc.iterator.Next())
			assert.NoError(t, tc.iterator.Error())
			assert.NoError(t, tc.iterator.Close())
		})
	}
}

func TestHeapIteratorPrefetch(t *testing.T) {
	t.Parallel()

	type tester func(t *testing.T, i HeapIterator)

	tests := map[string]tester{
		"prefetch on Len() when called as first method": func(t *testing.T, i HeapIterator) {
			assert.Equal(t, 2, i.Len())
		},
		"prefetch on Peek() when called as first method": func(t *testing.T, i HeapIterator) {
			assert.Equal(t, time.Unix(0, 0), i.Peek())
		},
		"prefetch on Next() when called as first method": func(t *testing.T, i HeapIterator) {
			assert.True(t, i.Next())
			assert.Equal(t, logproto.Entry{Timestamp: time.Unix(0, 0), Line: "0"}, i.Entry())
		},
	}

	for testName, testFunc := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			i := NewHeapIterator(context.Background(), []EntryIterator{
				mkStreamIterator(identity, "{foobar: \"baz1\"}"),
				mkStreamIterator(identity, "{foobar: \"baz2\"}"),
			}, logproto.FORWARD)

			testFunc(t, i)
		})
	}
}

type generator func(i int64) logproto.Entry

func mkStreamIterator(f generator, labels string) EntryIterator {
	entries := []logproto.Entry{}
	for i := range int64(testSize) {
		entries = append(entries, f(i))
	}
	return NewStreamIterator(logproto.Stream{
		Entries: entries,
		Labels:  labels,
	})
}

func identity(i int64) logproto.Entry {
	return logproto.Entry{
		Timestamp: time.Unix(i, 0),
		Line:      strconv.FormatInt(i, 10),
	}
}

func offset(j int64, g generator) generator {
	return func(i int64) logproto.Entry {
		return g(i + j)
	}
}

func constant(t int64) generator {
	return func(i int64) logproto.Entry {
		return logproto.Entry{
			Timestamp: time.Unix(t, 0),
			Line:      fmt.Sprintf("%d", i),
		}
	}
}

func inverse(g generator) generator {
	return func(i int64) logproto.Entry {
		return g(-i)
	}
}

func Test_DuplicateCount(t *testing.T) {
	stream := logproto.Stream{
		Entries: []logproto.Entry{
			{
				Timestamp: time.Unix(0, 1),
				Line:      "foo",
			},
			{
				Timestamp: time.Unix(0, 2),
				Line:      "foo",
			},
			{
				Timestamp: time.Unix(0, 3),
				Line:      "foo",
			},
		},
	}

	for _, test := range []struct {
		name               string
		iters              []EntryIterator
		direction          logproto.Direction
		expectedDuplicates int64
	}{
		{
			"empty b",
			[]EntryIterator{},
			logproto.BACKWARD,
			0,
		},
		{
			"empty f",
			[]EntryIterator{},
			logproto.FORWARD,
			0,
		},
		{
			"replication 2 b",
			[]EntryIterator{
				NewStreamIterator(stream),
				NewStreamIterator(stream),
			},
			logproto.BACKWARD,
			3,
		},
		{
			"replication 2 f",
			[]EntryIterator{
				NewStreamIterator(stream),
				NewStreamIterator(stream),
			},
			logproto.FORWARD,
			3,
		},
		{
			"replication 3 f",
			[]EntryIterator{
				NewStreamIterator(stream),
				NewStreamIterator(stream),
				NewStreamIterator(stream),
				NewStreamIterator(logproto.Stream{
					Entries: []logproto.Entry{
						{
							Timestamp: time.Unix(0, 4),
							Line:      "bar",
						},
					},
				}),
			},
			logproto.FORWARD,
			6,
		},
		{
			"replication 3 b",
			[]EntryIterator{
				NewStreamIterator(stream),
				NewStreamIterator(stream),
				NewStreamIterator(stream),
				NewStreamIterator(logproto.Stream{
					Entries: []logproto.Entry{
						{
							Timestamp: time.Unix(0, 4),
							Line:      "bar",
						},
					},
				}),
			},
			logproto.BACKWARD,
			6,
		},
		{
			"single f",
			[]EntryIterator{
				NewStreamIterator(logproto.Stream{
					Entries: []logproto.Entry{
						{
							Timestamp: time.Unix(0, 4),
							Line:      "bar",
						},
					},
				}),
			},
			logproto.FORWARD,
			0,
		},
		{
			"single b",
			[]EntryIterator{
				NewStreamIterator(logproto.Stream{
					Entries: []logproto.Entry{
						{
							Timestamp: time.Unix(0, 4),
							Line:      "bar",
						},
					},
				}),
			},
			logproto.BACKWARD,
			0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = stats.NewContext(ctx)
			it := NewHeapIterator(ctx, test.iters, test.direction)
			defer it.Close()
			for it.Next() {
			}
			require.Equal(t, test.expectedDuplicates, stats.GetChunkData(ctx).TotalDuplicates)
		})
	}
}
