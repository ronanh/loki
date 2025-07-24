package iter

import (
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql/stats"
)

// EntryIterator iterates over entries in time-order.
type EntryIterator interface {
	Next() bool
	Entry() logproto.Entry
	Labels() string
	Error() error
	Close() error
}

type noOpIterator struct{}

var NoopIterator = noOpIterator{}

func (noOpIterator) Next() bool              { return false }
func (noOpIterator) Error() error            { return nil }
func (noOpIterator) Labels() string          { return "" }
func (noOpIterator) Entry() logproto.Entry   { return logproto.Entry{} }
func (noOpIterator) Sample() logproto.Sample { return logproto.Sample{} }
func (noOpIterator) Close() error            { return nil }

// streamIterator iterates over entries in a stream.
type streamIterator struct {
	i       int
	entries []logproto.Entry
	labels  string
}

// NewStreamIterator iterates over entries in a stream.
func NewStreamIterator(stream logproto.Stream) EntryIterator {
	return &streamIterator{
		i:       -1,
		entries: stream.Entries,
		labels:  stream.Labels,
	}
}

func (i *streamIterator) Next() bool {
	i.i++
	return i.i < len(i.entries)
}

func (i *streamIterator) Error() error {
	return nil
}

func (i *streamIterator) Labels() string {
	return i.labels
}

func (i *streamIterator) Entry() logproto.Entry {
	return i.entries[i.i]
}

func (i *streamIterator) Close() error {
	return nil
}

// HeapIterator iterates over a heap of iterators with ability to push new iterators and get some
// properties like time of entry at peek and len
//
// Not safe for concurrent use.
type HeapIterator interface {
	EntryIterator
	Peek() time.Time
	Len() int
	Push(EntryIterator)
}

type mergingIterator struct {
	stats     *stats.ChunkData
	ctx       context.Context
	its       []EntryIterator
	curEntry  logproto.Entry
	curLabels string
	reversed  bool
	err       error
}

var (
	_ EntryIterator = (*mergingIterator)(nil)
	_ HeapIterator  = (*mergingIterator)(nil)
)

func NewHeapIterator(
	ctx context.Context,
	is []EntryIterator,
	direction logproto.Direction,
) HeapIterator {
	return NewMergingIterator(ctx, is, direction)
}

func NewMergingIterator(
	ctx context.Context,
	its []EntryIterator,
	direction logproto.Direction,
) HeapIterator {
	startedIts := make([]EntryIterator, 0, len(its))
	var err error
	for _, it := range its {
		if it.Next() {
			startedIts = append(startedIts, it)
		} else if err == nil {
			err = it.Error()
		}
	}

	mi := &mergingIterator{
		stats:    stats.GetChunkData(ctx),
		ctx:      ctx,
		its:      startedIts,
		reversed: direction == logproto.BACKWARD,
		err:      err,
	}
	sort.Slice(mi.its, mi.less)

	return mi
}

// Close closes the iterator and frees associated ressources.
func (mi *mergingIterator) Close() error {
	for _, it := range mi.its {
		if it != nil {
			if err := it.Close(); err != nil {
				return err
			}
		}
	}
	mi.its = nil
	return nil
}

// Error returns errors encountered by the iterator.
func (mi *mergingIterator) Error() error {
	if mi.err != nil {
		return mi.err
	}
	return mi.ctx.Err()
}

func (mi *mergingIterator) Len() int {
	return len(mi.its)
}

func (mi *mergingIterator) Push(it EntryIterator) {
	if it.Next() {
		mi.its = append(mi.its, it)
		sort.Slice(mi.its, mi.less)
	}
}

func (mi *mergingIterator) Peek() time.Time {
	if len(mi.its) == 0 {
		return time.Time{}
	}

	return mi.its[0].Entry().Timestamp
}

func (mi *mergingIterator) Entry() logproto.Entry {
	return mi.curEntry
}

func (mi *mergingIterator) Labels() string {
	return mi.curLabels
}

func (mi *mergingIterator) less(i, j int) bool {
	ts1, ts2 := mi.its[i].Entry().Timestamp.UnixNano(), mi.its[j].Entry().Timestamp.UnixNano()
	if ts1 != ts2 {
		return ts1 < ts2 != mi.reversed
	}
	if mi.its[i].Labels() != mi.its[j].Labels() {
		return mi.its[i].Labels() < mi.its[j].Labels()
	}
	return mi.its[i].Entry().Line < mi.its[j].Entry().Line
}

func (mi *mergingIterator) Next() bool {
	if len(mi.its) == 0 {
		return false
	}

	// set current entry to next entry
	mi0 := mi.its[0]
	mi.curEntry, mi.curLabels = mi0.Entry(), mi0.Labels()

	needSort := mi.dedup()

	// advance iterator
	if !mi0.Next() {
		// stream finished: remove it
		mi0.Close()
		mi.its[0] = nil
		mi0 = nil
		mi.its = mi.its[1:]
		if len(mi.its) > 0 {
			mi0 = mi.its[0]
		}
	}
	if len(mi.its) == 0 {
		return true
	}
	if needSort {
		sort.Slice(mi.its, mi.less)
	} else {
		// Ensure streams sorted (only sort the stream that was advanced)
		var firstItNewPos int
		for firstItNewPos = 1; firstItNewPos < len(mi.its); firstItNewPos++ {
			if !mi.less(firstItNewPos, firstItNewPos-1) {
				firstItNewPos--
				break
			}
		}
		if firstItNewPos == len(mi.its) {
			firstItNewPos--
		}
		switch firstItNewPos {
		case 0:
			// nothing to do
		case 1:
			// swap the first two elements
			mi.its[0], mi.its[1] = mi.its[1], mi0
		default:
			// copy the first element and shift the rest
			copy(mi.its, mi.its[1:firstItNewPos+1])
			mi.its[firstItNewPos] = mi0
		}
	}

	return true
}

func (mi *mergingIterator) dedup() bool {
	// Ensure no duplicates
	var needSort bool
	for i := 1; i < len(mi.its); i++ {
		if mi.its[i].Entry().Timestamp != mi.its[0].Entry().Timestamp ||
			mi.its[i].Labels() != mi.its[0].Labels() {
			break
		}
		// Duplicate -> advance to discard
		mi.stats.TotalDuplicates++
		if !mi.its[i].Next() {
			// stream finished: remove it
			mi.its[i].Close()
			mi.its[i] = nil
			mi.its = append(mi.its[:i], mi.its[i+1:]...)
			// restart iteration from the same position
			i--
			continue
		}
		needSort = true
	}
	return needSort
}

// NewStreamsIterator returns an iterator over logproto.Stream.
func NewStreamsIterator(
	ctx context.Context,
	streams []logproto.Stream,
	direction logproto.Direction,
) EntryIterator {
	is := make([]EntryIterator, 0, len(streams))
	for i := range streams {
		is = append(is, NewStreamIterator(streams[i]))
	}
	return NewHeapIterator(ctx, is, direction)
}

// NewQueryResponseIterator returns an iterator over a QueryResponse.
func NewQueryResponseIterator(
	ctx context.Context,
	resp *logproto.QueryResponse,
	direction logproto.Direction,
) EntryIterator {
	is := make([]EntryIterator, 0, len(resp.Streams))
	for i := range resp.Streams {
		is = append(is, NewStreamIterator(resp.Streams[i]))
	}
	return NewHeapIterator(ctx, is, direction)
}

type queryClientIterator struct {
	client    logproto.Querier_QueryClient
	direction logproto.Direction
	err       error
	curr      EntryIterator
}

// NewQueryClientIterator returns an iterator over a QueryClient.
func NewQueryClientIterator(
	client logproto.Querier_QueryClient,
	direction logproto.Direction,
) EntryIterator {
	return &queryClientIterator{
		client:    client,
		direction: direction,
	}
}

func (i *queryClientIterator) Next() bool {
	for i.curr == nil || !i.curr.Next() {
		batch, err := i.client.Recv()
		if errors.Is(err, io.EOF) {
			return false
		} else if err != nil {
			i.err = err
			return false
		}

		i.curr = NewQueryResponseIterator(i.client.Context(), batch, i.direction)
	}

	return true
}

func (i *queryClientIterator) Entry() logproto.Entry {
	return i.curr.Entry()
}

func (i *queryClientIterator) Labels() string {
	return i.curr.Labels()
}

func (i *queryClientIterator) Error() error {
	return i.err
}

func (i *queryClientIterator) Close() error {
	return i.client.CloseSend()
}
