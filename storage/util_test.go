package storage

import (
	"context"
	"time"

	util_log "github.com/cortexproject/cortex/pkg/util/log"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/ronanh/loki/chunkenc"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql"
)

var fooLabelsWithName = "{foo=\"bar\", __name__=\"logs\"}"
var fooLabels = "{foo=\"bar\"}"

var from = time.Unix(0, time.Millisecond.Nanoseconds())

func newChunk(stream logproto.Stream) chunk.Chunk {
	lbs, err := logql.ParseLabels(stream.Labels)
	if err != nil {
		panic(err)
	}
	if !lbs.Has(labels.MetricName) {
		builder := labels.NewBuilder(lbs)
		builder.Set(labels.MetricName, "logs")
		lbs = builder.Labels()
	}
	from, through := model.TimeFromUnixNano(stream.Entries[0].Timestamp.UnixNano()), model.TimeFromUnixNano(stream.Entries[0].Timestamp.UnixNano())
	chk := chunkenc.NewMemChunk(chunkenc.EncGZIP, 256*1024, 0)
	for _, e := range stream.Entries {
		if e.Timestamp.UnixNano() < from.UnixNano() {
			from = model.TimeFromUnixNano(e.Timestamp.UnixNano())
		}
		if e.Timestamp.UnixNano() > through.UnixNano() {
			through = model.TimeFromUnixNano(e.Timestamp.UnixNano())
		}
		_ = chk.Append(&e)
	}
	chk.Close()
	c := chunk.NewChunk("fake", client.Fingerprint(lbs), lbs, chunkenc.NewFacade(chk, 0, 0), from, through)
	// force the checksum creation
	if err := c.Encode(); err != nil {
		panic(err)
	}
	return c
}

type mockChunkStore struct {
	chunks []chunk.Chunk
	client *mockChunkStoreClient
}

// mockChunkStore cannot implement both chunk.Store and chunk.Client,
// since there is a conflict in signature for DeleteChunk method.
var _ chunk.Store = &mockChunkStore{}
var _ chunk.Client = &mockChunkStoreClient{}

func newMockChunkStore(streams []*logproto.Stream) *mockChunkStore {
	chunks := make([]chunk.Chunk, 0, len(streams))
	for _, s := range streams {
		chunks = append(chunks, newChunk(*s))
	}
	return &mockChunkStore{chunks: chunks, client: &mockChunkStoreClient{chunks: chunks}}
}

func (m *mockChunkStore) Put(ctx context.Context, chunks []chunk.Chunk) error { return nil }
func (m *mockChunkStore) PutOne(ctx context.Context, from, through model.Time, chunk chunk.Chunk) error {
	return nil
}
func (m *mockChunkStore) LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string) ([]string, error) {
	return nil, nil
}
func (m *mockChunkStore) LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string) ([]string, error) {
	return nil, nil
}

func (m *mockChunkStore) DeleteChunk(ctx context.Context, from, through model.Time, userID, chunkID string, metric labels.Labels, partiallyDeletedInterval *model.Interval) error {
	return nil
}

func (m *mockChunkStore) DeleteSeriesIDs(ctx context.Context, from, through model.Time, userID string, metric labels.Labels) error {
	return nil
}
func (m *mockChunkStore) Stop() {}
func (m *mockChunkStore) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	return nil, nil
}
func (m *mockChunkStore) GetChunkFetcher(_ model.Time) *chunk.Fetcher {
	return nil
}

func (m *mockChunkStore) GetChunkRefs(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([][]chunk.Chunk, []*chunk.Fetcher, error) {
	refs := make([]chunk.Chunk, 0, len(m.chunks))
	// transform real chunks into ref chunks.
	for _, c := range m.chunks {
		r, err := chunk.ParseExternalKey("fake", c.ExternalKey())
		if err != nil {
			panic(err)
		}
		refs = append(refs, r)
	}

	cache, err := cache.New(cache.Config{Prefix: "chunks"}, nil, util_log.Logger)
	if err != nil {
		panic(err)
	}

	f, err := chunk.NewChunkFetcher(cache, false, m.client)
	if err != nil {
		panic(err)
	}
	return [][]chunk.Chunk{refs}, []*chunk.Fetcher{f}, nil
}

type mockChunkStoreClient struct {
	chunks []chunk.Chunk
}

func (m mockChunkStoreClient) Stop() {
	panic("implement me")
}

func (m mockChunkStoreClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	return nil
}

func (m mockChunkStoreClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	var res []chunk.Chunk
	for _, c := range chunks {
		for _, sc := range m.chunks {
			// only returns chunks requested using the external key
			if c.ExternalKey() == sc.ExternalKey() {
				res = append(res, sc)
			}
		}
	}
	return res, nil
}

func (m mockChunkStoreClient) DeleteChunk(ctx context.Context, userID, chunkID string) error {
	return nil
}

var streamsFixture = []*logproto.Stream{
	{
		Labels: "{foo=\"bar\"}",
		Entries: []logproto.Entry{
			{
				Timestamp: from,
				Line:      "1",
			},

			{
				Timestamp: from.Add(time.Millisecond),
				Line:      "2",
			},
			{
				Timestamp: from.Add(2 * time.Millisecond),
				Line:      "3",
			},
		},
	},
	{
		Labels: "{foo=\"bar\"}",
		Entries: []logproto.Entry{
			{
				Timestamp: from.Add(2 * time.Millisecond),
				Line:      "3",
			},
			{
				Timestamp: from.Add(3 * time.Millisecond),
				Line:      "4",
			},

			{
				Timestamp: from.Add(4 * time.Millisecond),
				Line:      "5",
			},
			{
				Timestamp: from.Add(5 * time.Millisecond),
				Line:      "6",
			},
		},
	},
	{
		Labels: "{foo=\"bazz\"}",
		Entries: []logproto.Entry{
			{
				Timestamp: from,
				Line:      "1",
			},

			{
				Timestamp: from.Add(time.Millisecond),
				Line:      "2",
			},
			{
				Timestamp: from.Add(2 * time.Millisecond),
				Line:      "3",
			},
		},
	},
	{
		Labels: "{foo=\"bazz\"}",
		Entries: []logproto.Entry{
			{
				Timestamp: from.Add(2 * time.Millisecond),
				Line:      "3",
			},
			{
				Timestamp: from.Add(3 * time.Millisecond),
				Line:      "4",
			},

			{
				Timestamp: from.Add(4 * time.Millisecond),
				Line:      "5",
			},
			{
				Timestamp: from.Add(5 * time.Millisecond),
				Line:      "6",
			},
		},
	},
}
var storeFixture = newMockChunkStore(streamsFixture)
