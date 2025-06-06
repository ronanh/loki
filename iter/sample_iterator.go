package iter

import (
	"container/heap"
	"context"
	"io"
	"sort"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/ronanh/loki/helpers"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql/stats"
	"github.com/ronanh/loki/util"
)

// SampleIterator iterates over samples in time-order.
type SampleIterator interface {
	Next() bool
	// todo(ctovena) we should add `Seek(t int64) bool`
	// This way we can skip when ranging over samples.
	Sample() logproto.Sample
	Labels() string
	Error() error
	Close() error
}

type Seekable interface {
	Seek(t int64) bool
}

type PromLabels interface {
	PromLabels() labels.Labels
}

type PeekPromLabels interface {
	PeekPromLabels() labels.Labels
}

// PeekingSampleIterator is a sample iterator that can peek sample without moving the current sample.
type PeekingSampleIterator interface {
	SampleIterator
	Peek() (string, logproto.Sample, bool)
}

type peekingSampleIterator struct {
	iter SampleIterator

	cache *sampleWithLabels
	next  *sampleWithLabels
}

type sampleWithLabels struct {
	logproto.Sample
	labels string
}

func NewPeekingSampleIteratorLoki(iter SampleIterator) PeekingSampleIterator {
	// initialize the next entry so we can peek right from the start.
	var cache *sampleWithLabels
	next := &sampleWithLabels{}
	if iter.Next() {
		cache = &sampleWithLabels{
			Sample: iter.Sample(),
			labels: iter.Labels(),
		}
		next.Sample = cache.Sample
		next.labels = cache.labels
	}
	return &peekingSampleIterator{
		iter:  iter,
		cache: cache,
		next:  next,
	}
}

func (it *peekingSampleIterator) Close() error {
	return it.iter.Close()
}

func (it *peekingSampleIterator) Labels() string {
	if it.next != nil {
		return it.next.labels
	}
	return ""
}

func (it *peekingSampleIterator) Next() bool {
	if it.cache != nil {
		it.next.Sample = it.cache.Sample
		it.next.labels = it.cache.labels
		it.cacheNext()
		return true
	}
	return false
}

// cacheNext caches the next element if it exists.
func (it *peekingSampleIterator) cacheNext() {
	if it.iter.Next() {
		it.cache.Sample = it.iter.Sample()
		it.cache.labels = it.iter.Labels()
		return
	}
	// nothing left removes the cached entry
	it.cache = nil
}

func (it *peekingSampleIterator) Sample() logproto.Sample {
	if it.next != nil {
		return it.next.Sample
	}
	return logproto.Sample{}
}

func (it *peekingSampleIterator) Peek() (string, logproto.Sample, bool) {
	if it.cache != nil {
		return it.cache.labels, it.cache.Sample, true
	}
	return "", logproto.Sample{}, false
}

func (it *peekingSampleIterator) Error() error {
	return it.iter.Error()
}

type sampleIteratorHeap []SampleIterator

func (h sampleIteratorHeap) Len() int             { return len(h) }
func (h sampleIteratorHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h sampleIteratorHeap) Peek() SampleIterator { return h[0] }
func (h *sampleIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(SampleIterator))
}

func (h *sampleIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h sampleIteratorHeap) Less(i, j int) bool {
	s1, s2 := h[i].Sample(), h[j].Sample()
	switch {
	case s1.Timestamp < s2.Timestamp:
		return true
	case s1.Timestamp > s2.Timestamp:
		return false
	default:
		return h[i].Labels() < h[j].Labels()
	}
}

// heapSampleIterator iterates over a heap of iterators.
type heapSampleIterator struct {
	heap       *sampleIteratorHeap
	is         []SampleIterator
	prefetched bool
	stats      *stats.ChunkData

	tuples     []sampletuple
	curr       logproto.Sample
	currLabels string
	errs       []error
}

// NewHeapSampleIterator returns a new iterator which uses a heap to merge together
// entries for multiple iterators.
func NewHeapSampleIteratorLoki(ctx context.Context, is []SampleIterator) SampleIterator {
	return &heapSampleIterator{
		stats:  stats.GetChunkData(ctx),
		is:     is,
		heap:   &sampleIteratorHeap{},
		tuples: make([]sampletuple, 0, len(is)),
	}
}

// prefetch iterates over all inner iterators to merge together, calls Next() on
// each of them to prefetch the first entry and pushes of them - who are not
// empty - to the heap
func (i *heapSampleIterator) prefetch() {
	if i.prefetched {
		return
	}

	i.prefetched = true
	for _, it := range i.is {
		i.requeue(it, false)
	}

	// We can now clear the list of input iterators to merge, given they have all
	// been processed and the non empty ones have been pushed to the heap
	i.is = nil
}

// requeue pushes the input ei EntryIterator to the heap, advancing it via an ei.Next()
// call unless the advanced input parameter is true. In this latter case it expects that
// the iterator has already been advanced before calling requeue().
//
// If the iterator has no more entries or an error occur while advancing it, the iterator
// is not pushed to the heap and any possible error captured, so that can be get via Error().
func (i *heapSampleIterator) requeue(ei SampleIterator, advanced bool) {
	if advanced || ei.Next() {
		heap.Push(i.heap, ei)
		return
	}

	if err := ei.Error(); err != nil {
		i.errs = append(i.errs, err)
	}
	helpers.LogError("closing iterator", ei.Close)
}

type sampletuple struct {
	logproto.Sample
	SampleIterator
}

func (i *heapSampleIterator) Next() bool {
	i.prefetch()

	if i.heap.Len() == 0 {
		return false
	}

	// We support multiple entries with the same timestamp, and we want to
	// preserve their original order. We look at all the top entries in the
	// heap with the same timestamp, and pop the ones whose common value
	// occurs most often.
	for i.heap.Len() > 0 {
		next := i.heap.Peek()
		sample := next.Sample()
		if len(i.tuples) > 0 && (i.tuples[0].Labels() != next.Labels() || i.tuples[0].Timestamp != sample.Timestamp) {
			break
		}

		heap.Pop(i.heap)
		i.tuples = append(i.tuples, sampletuple{
			Sample:         sample,
			SampleIterator: next,
		})
	}

	i.curr = i.tuples[0].Sample
	i.currLabels = i.tuples[0].Labels()
	t := i.tuples[0]
	if len(i.tuples) == 1 {
		i.requeue(i.tuples[0].SampleIterator, false)
		i.tuples = i.tuples[:0]
		return true
	}
	// Requeue the iterators, advancing them if they were consumed.
	for j := range i.tuples {
		if i.tuples[j].Hash != i.curr.Hash {
			i.requeue(i.tuples[j].SampleIterator, true)
			continue
		}
		// we count as duplicates only if the tuple is not the one (t) used to fill the current entry
		if i.tuples[j] != t {
			i.stats.TotalDuplicates++
		}
		i.requeue(i.tuples[j].SampleIterator, false)
	}
	i.tuples = i.tuples[:0]
	return true
}

func (i *heapSampleIterator) Sample() logproto.Sample {
	return i.curr
}

func (i *heapSampleIterator) Labels() string {
	return i.currLabels
}

func (i *heapSampleIterator) Error() error {
	switch len(i.errs) {
	case 0:
		return nil
	case 1:
		return i.errs[0]
	default:
		return util.MultiError(i.errs)
	}
}

func (i *heapSampleIterator) Close() error {
	for i.heap.Len() > 0 {
		if err := i.heap.Pop().(SampleIterator).Close(); err != nil {
			return err
		}
	}
	i.tuples = nil
	return nil
}

type mergingSampleIterator struct {
	stats      *stats.ChunkData
	ctx        context.Context
	iActiveIts []int
	its        []struct {
		SampleIterator
		*logproto.Sample
		labels string
	}
	curSample logproto.Sample
	curLabels string
	errs      []error
}

var _ SampleIterator = (*mergingSampleIterator)(nil)
var _ PeekingSampleIterator = (*mergingSampleIterator)(nil)

func NewHeapSampleIterator(ctx context.Context, is []SampleIterator) SampleIterator {
	return NewMergingSampleIterator(ctx, is)
}

func NewPeekingSampleIterator(iter SampleIterator) PeekingSampleIterator {
	if pki, ok := iter.(PeekingSampleIterator); ok {
		return pki
	}
	return NewMergingSampleIterator(context.Background(), []SampleIterator{iter})
}

func NewMergingSampleIterator(ctx context.Context, its []SampleIterator) PeekingSampleIterator {
	startedIts := make([]struct {
		SampleIterator
		*logproto.Sample
		labels string
	}, 0, len(its))
	startedItsSamples := make([]logproto.Sample, 0, len(its))
	iActiveIts := make([]int, 0, len(its))
	var errs []error
	for _, it := range its {
		hasNext := it.Next()
		if err := it.Error(); err != nil {
			errs = append(errs, err)
		}
		if hasNext {
			startedItsSamples = append(startedItsSamples, it.Sample())
			startedIts = append(startedIts, struct {
				SampleIterator
				*logproto.Sample
				labels string
			}{it, &startedItsSamples[len(startedItsSamples)-1], it.Labels()})
			iActiveIts = append(iActiveIts, len(startedIts)-1)
		}
	}

	mi := &mergingSampleIterator{
		stats:      stats.GetChunkData(ctx),
		ctx:        ctx,
		its:        startedIts,
		iActiveIts: iActiveIts,
		errs:       errs,
	}
	sort.Slice(mi.iActiveIts, mi.less)

	return mi
}

var _ SampleIterator = (*mergingSampleIterator)(nil)
var _ PeekPromLabels = (*mergingSampleIterator)(nil)
var _ Seekable = (*mergingSampleIterator)(nil)

// Close closes the iterator and frees associated ressources
func (mi *mergingSampleIterator) Close() error {
	for _, it := range mi.its {
		if it.SampleIterator != nil {
			if err := it.Close(); err != nil {
				return err
			}
		}
	}
	mi.its = nil
	mi.iActiveIts = nil
	return nil
}

// Error returns errors encountered by the iterator
func (mi *mergingSampleIterator) Error() error {
	switch len(mi.errs) {
	case 0:
		return nil
	case 1:
		return mi.errs[0]
	default:
		return util.MultiError(mi.errs)
	}
}

func (mi *mergingSampleIterator) Peek() (string, logproto.Sample, bool) {
	if len(mi.iActiveIts) == 0 {
		return "", logproto.Sample{}, false
	}
	mits := &mi.its[mi.iActiveIts[0]]
	return mits.labels, *mits.Sample, true
}

func (mi *mergingSampleIterator) Sample() logproto.Sample {
	return mi.curSample
}

func (mi *mergingSampleIterator) Labels() string {
	return mi.curLabels
}

func (mi *mergingSampleIterator) PeekPromLabels() labels.Labels {
	if len(mi.iActiveIts) == 0 {
		return nil
	}
	if pl, ok := mi.its[mi.iActiveIts[0]].SampleIterator.(PromLabels); ok {
		return pl.PromLabels()
	}
	return nil
}

func (mi *mergingSampleIterator) less(i, j int) bool {
	ii, ij := mi.iActiveIts[i], mi.iActiveIts[j]
	s1, s2 := mi.its[ii].Sample, mi.its[ij].Sample
	switch {
	case s1.Timestamp < s2.Timestamp:
		return true
	case s1.Timestamp > s2.Timestamp:
		return false
	default:
		lbls1, lbls2 := mi.its[ii].labels, mi.its[ij].labels
		if len(lbls1) != len(lbls2) {
			// fast path for different lengths
			return lbls1 < lbls2
		}
		if lbls1 == lbls2 {
			return s1.Hash < s2.Hash
		}
		return lbls1 < lbls2
	}
}

func (mi *mergingSampleIterator) Next() bool {
	if len(mi.iActiveIts) == 0 {
		mi.curSample, mi.curLabels = logproto.Sample{}, ""
		return false
	}
	mi.dedup()

	its0 := &mi.its[mi.iActiveIts[0]]
	// set current sample to next sample
	mi.curSample, mi.curLabels = *its0.Sample, its0.labels

	// advance iterator
	hasNext := its0.SampleIterator.Next()
	if err := its0.SampleIterator.Error(); err != nil {
		mi.errs = append(mi.errs, err)
	}
	if !hasNext {
		// stream finished: remove it
		its0.SampleIterator.Close()
		its0.SampleIterator = nil
		mi.iActiveIts = mi.iActiveIts[1:]
	} else {
		*its0.Sample = its0.SampleIterator.Sample()
		ts0 := its0.Timestamp
		its0.labels = its0.SampleIterator.Labels()

		// Ensure streams sorted (only sort the stream that was advanced)
		var firstItNewPos int
		for firstItNewPos = 1; firstItNewPos < len(mi.iActiveIts); firstItNewPos++ {
			firstItNewPosVal := mi.iActiveIts[firstItNewPos]
			ts := mi.its[firstItNewPosVal].Timestamp
			if ts < ts0 { // fast path for less
				continue
			}
			if ts > ts0 || // fast path for greater
				!mi.less(firstItNewPos, 0) {
				firstItNewPos--
				break
			}
		}
		if firstItNewPos == len(mi.iActiveIts) {
			firstItNewPos--
		}
		switch firstItNewPos {
		case 0:
			// nothing to do
		case 1:
			// swap the first two elements
			mi.iActiveIts[0], mi.iActiveIts[1] = mi.iActiveIts[1], mi.iActiveIts[0]
		default:
			// copy the first element and shift the rest
			msi0 := mi.iActiveIts[0]
			copy(mi.iActiveIts, mi.iActiveIts[1:firstItNewPos+1])
			mi.iActiveIts[firstItNewPos] = msi0
		}
	}
	return true
}

func (mi *mergingSampleIterator) dedup() {
	if len(mi.iActiveIts) == 0 {
		return
	}
	its0 := &mi.its[mi.iActiveIts[0]]
	// Ensure no duplicates
	for i := 1; i < len(mi.iActiveIts); i++ {
		itsi := &mi.its[mi.iActiveIts[i]]
		if itsi.Sample.Timestamp != its0.Sample.Timestamp || itsi.labels != its0.labels || itsi.Sample.Hash != its0.Sample.Hash {
			break
		}
		// Duplicate -> advance to discard
		mi.stats.TotalDuplicates++
		hasNext := itsi.SampleIterator.Next()
		if err := its0.SampleIterator.Error(); err != nil {
			mi.errs = append(mi.errs, err)
		}
		if !hasNext {
			// stream finished: remove it
			itsi.Close()
			itsi.SampleIterator = nil
			mi.iActiveIts = append(mi.iActiveIts[:i], mi.iActiveIts[i+1:]...)
			// restart iteration from the same position
			i--
			continue
		}
		*itsi.Sample = itsi.SampleIterator.Sample()
		itsi.labels = itsi.SampleIterator.Labels()
		// Ensure sorted
		for j := i + 1; j < len(mi.iActiveIts); j++ {
			if !mi.less(j, j-1) {
				break
			}
			mi.iActiveIts[j-1], mi.iActiveIts[j] = mi.iActiveIts[j], mi.iActiveIts[j-1]
		}
		// restart iteration from the same position
		i--
	}
}

func (mi *mergingSampleIterator) Seek(t int64) bool {
	// special case: current sample (for its[0])
	if mi.curSample.Timestamp != 0 && mi.curSample.Timestamp >= t {
		// no need to advance
		return true
	}
	var (
		j        int
		needSort bool
	)
	for i := 0; i < len(mi.iActiveIts); i++ {
		itsi := &mi.its[mi.iActiveIts[i]]
		if s, ok := itsi.SampleIterator.(Seekable); ok {
			hasNext := s.Seek(t)
			if err := itsi.SampleIterator.Error(); err != nil {
				mi.errs = append(mi.errs, err)
			}
			if !hasNext {
				// stream finished: remove it
				itsi.Close()
				itsi.SampleIterator = nil
				continue
			}
			sample := itsi.SampleIterator.Sample()
			if *itsi.Sample != sample { // compare previous sample to avoid sorting if possible
				needSort = true
			}
			*itsi.Sample = sample
			itsi.labels = itsi.SampleIterator.Labels()
			mi.iActiveIts[j] = mi.iActiveIts[i]
			j++
		} else {
			// Seek not supported: use Next() to advance
			var advanced bool
			for itsi.Sample.Timestamp < t {
				hasNext := itsi.SampleIterator.Next()
				if err := itsi.SampleIterator.Error(); err != nil {
					mi.errs = append(mi.errs, err)
				}
				if !hasNext {
					// stream finished: remove it
					itsi.Close()
					itsi.SampleIterator = nil
					break
				}
				*itsi.Sample = itsi.SampleIterator.Sample()
				itsi.labels = itsi.SampleIterator.Labels()
				advanced = true
			}
			if itsi.SampleIterator != nil {
				mi.iActiveIts[j] = mi.iActiveIts[i]
				if advanced {
					needSort = true
					*itsi.Sample = itsi.SampleIterator.Sample()
					itsi.labels = itsi.SampleIterator.Labels()
				}
				j++
			}
		}
	}
	mi.iActiveIts = mi.iActiveIts[:j]
	if len(mi.iActiveIts) == 0 {
		mi.curSample, mi.curLabels = logproto.Sample{}, ""
		return false
	}
	if needSort {
		sort.Slice(mi.iActiveIts, mi.less)
	}

	// set current sample to next sample
	mi.curSample, mi.curLabels = *mi.its[mi.iActiveIts[0]].Sample, mi.its[mi.iActiveIts[0]].labels
	return true
}

type sampleQueryClientIterator struct {
	client QuerySampleClient
	err    error
	curr   SampleIterator
}

// QuerySampleClient is GRPC stream client with only method used by the SampleQueryClientIterator
type QuerySampleClient interface {
	Recv() (*logproto.SampleQueryResponse, error)
	Context() context.Context
	CloseSend() error
}

// NewQueryClientIterator returns an iterator over a QueryClient.
func NewSampleQueryClientIterator(client QuerySampleClient) SampleIterator {
	return &sampleQueryClientIterator{
		client: client,
	}
}

func (i *sampleQueryClientIterator) Next() bool {
	for i.curr == nil || !i.curr.Next() {
		batch, err := i.client.Recv()
		if err == io.EOF {
			return false
		} else if err != nil {
			i.err = err
			return false
		}

		i.curr = NewSampleQueryResponseIterator(i.client.Context(), batch)
	}

	return true
}

func (i *sampleQueryClientIterator) Sample() logproto.Sample {
	return i.curr.Sample()
}

func (i *sampleQueryClientIterator) Labels() string {
	return i.curr.Labels()
}

func (i *sampleQueryClientIterator) Error() error {
	return i.err
}

func (i *sampleQueryClientIterator) Close() error {
	return i.client.CloseSend()
}

// NewSampleQueryResponseIterator returns an iterator over a SampleQueryResponse.
func NewSampleQueryResponseIterator(ctx context.Context, resp *logproto.SampleQueryResponse) SampleIterator {
	return NewMultiSeriesIterator(ctx, resp.Series)
}

type seriesIterator struct {
	i       int
	samples []logproto.Sample
	labels  string
}

// NewMultiSeriesIterator returns an iterator over multiple logproto.Series
func NewMultiSeriesIterator(ctx context.Context, series []logproto.Series) SampleIterator {
	is := make([]SampleIterator, 0, len(series))
	for i := range series {
		is = append(is, NewSeriesIterator(series[i]))
	}
	return NewHeapSampleIterator(ctx, is)
}

// NewSeriesIterator iterates over sample in a series.
func NewSeriesIterator(series logproto.Series) SampleIterator {
	return &seriesIterator{
		i:       -1,
		samples: series.Samples,
		labels:  series.Labels,
	}
}

func (i *seriesIterator) Next() bool {
	i.i++
	return i.i < len(i.samples)
}

func (i *seriesIterator) Error() error {
	return nil
}

func (i *seriesIterator) Labels() string {
	return i.labels
}

func (i *seriesIterator) Sample() logproto.Sample {
	return i.samples[i.i]
}

func (i *seriesIterator) Close() error {
	return nil
}

type nonOverlappingSampleIterator struct {
	labels    string
	i         int
	iterators []SampleIterator
	curr      SampleIterator
}

// NewNonOverlappingSampleIterator gives a chained iterator over a list of iterators.
func NewNonOverlappingSampleIterator(iterators []SampleIterator, labels string) SampleIterator {
	return &nonOverlappingSampleIterator{
		labels:    labels,
		iterators: iterators,
	}
}

func (i *nonOverlappingSampleIterator) Next() bool {
	for i.curr == nil || !i.curr.Next() {
		if len(i.iterators) == 0 {
			if i.curr != nil {
				i.curr.Close()
			}
			return false
		}
		if i.curr != nil {
			i.curr.Close()
		}
		i.i++
		i.curr, i.iterators = i.iterators[0], i.iterators[1:]
	}

	return true
}

func (i *nonOverlappingSampleIterator) Sample() logproto.Sample {
	return i.curr.Sample()
}

func (i *nonOverlappingSampleIterator) Labels() string {
	if i.labels != "" {
		return i.labels
	}

	return i.curr.Labels()
}

func (i *nonOverlappingSampleIterator) Error() error {
	if i.curr != nil {
		return i.curr.Error()
	}
	return nil
}

func (i *nonOverlappingSampleIterator) Close() error {
	for _, iter := range i.iterators {
		iter.Close()
	}
	i.iterators = nil
	return nil
}

type timeRangedSampleIterator struct {
	SampleIterator
	mint, maxt int64
}

// NewTimeRangedSampleIterator returns an iterator which filters entries by time range.
func NewTimeRangedSampleIterator(it SampleIterator, mint, maxt int64) SampleIterator {
	return &timeRangedSampleIterator{
		SampleIterator: it,
		mint:           mint,
		maxt:           maxt,
	}
}

func (i *timeRangedSampleIterator) Next() bool {
	ok := i.SampleIterator.Next()
	if !ok {
		i.SampleIterator.Close()
		return ok
	}
	ts := i.SampleIterator.Sample().Timestamp
	for ok && i.mint > ts {
		ok = i.SampleIterator.Next()
		if !ok {
			continue
		}
		ts = i.SampleIterator.Sample().Timestamp
	}
	if ok {
		if ts == i.mint { // The mint is inclusive
			return true
		}
		if i.maxt < ts || i.maxt == ts { // The maxt is exclusive.
			ok = false
		}
	}
	if !ok {
		i.SampleIterator.Close()
	}
	return ok
}

// ReadBatch reads a set of entries off an iterator.
func ReadSampleBatch(i SampleIterator, size uint32) (*logproto.SampleQueryResponse, uint32, error) {
	series := map[string]*logproto.Series{}
	respSize := uint32(0)
	for ; respSize < size && i.Next(); respSize++ {
		labels, sample := i.Labels(), i.Sample()
		s, ok := series[labels]
		if !ok {
			s = &logproto.Series{
				Labels: labels,
			}
			series[labels] = s
		}
		s.Samples = append(s.Samples, sample)
	}

	result := logproto.SampleQueryResponse{
		Series: make([]logproto.Series, 0, len(series)),
	}
	for _, s := range series {
		result.Series = append(result.Series, *s)
	}
	return &result, respSize, i.Error()
}
