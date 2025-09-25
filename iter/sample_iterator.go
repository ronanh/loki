package iter

import (
	"context"
	"io"
	"slices"
	"sort"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/ronanh/loki/logql/stats"
	"github.com/ronanh/loki/model"
	"github.com/ronanh/loki/util"
)

// SampleIterator iterates over samples in time-order.
type SampleIterator interface {
	Next() bool
	// todo(ctovena) we should add `Seek(t int64) bool`
	// This way we can skip when ranging over samples.
	Sample() model.Sample
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

// PeekingSampleIterator is a sample iterator that can peek sample without moving the current
// sample.
type PeekingSampleIterator interface {
	SampleIterator
	Peek() (string, model.Sample, bool)
}

type mergingSampleIterator struct {
	stats      *stats.ChunkData
	ctx        context.Context
	iActiveIts []int
	its        []struct {
		SampleIterator
		*model.Sample
		labels string
	}
	curSample model.Sample
	curLabels string
	errs      []error
}

var (
	_ SampleIterator        = (*mergingSampleIterator)(nil)
	_ PeekingSampleIterator = (*mergingSampleIterator)(nil)
)

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
		*model.Sample
		labels string
	}, 0, len(its))
	startedItsSamples := make([]model.Sample, 0, len(its))
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
				*model.Sample
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

var (
	_ SampleIterator = (*mergingSampleIterator)(nil)
	_ PeekPromLabels = (*mergingSampleIterator)(nil)
	_ Seekable       = (*mergingSampleIterator)(nil)
)

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

func (mi *mergingSampleIterator) Peek() (string, model.Sample, bool) {
	if len(mi.iActiveIts) == 0 {
		return "", model.Sample{}, false
	}
	mits := &mi.its[mi.iActiveIts[0]]
	return mits.labels, *mits.Sample, true
}

func (mi *mergingSampleIterator) Sample() model.Sample {
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
		mi.curSample, mi.curLabels = model.Sample{}, ""
		return false
	}
	mi.dedup()

	its0 := &mi.its[mi.iActiveIts[0]]
	// set current sample to next sample
	mi.curSample, mi.curLabels = *its0.Sample, its0.labels

	// advance iterator
	hasNext := its0.Next()
	if err := its0.Error(); err != nil {
		mi.errs = append(mi.errs, err)
	}
	if !hasNext {
		// stream finished: remove it
		its0.Close()
		its0.SampleIterator = nil
		mi.iActiveIts = mi.iActiveIts[1:]
	} else {
		*its0.Sample = its0.SampleIterator.Sample()
		ts0 := its0.Timestamp
		its0.labels = its0.Labels()

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
		if itsi.Timestamp != its0.Timestamp || itsi.labels != its0.labels ||
			itsi.Hash != its0.Hash {
			break
		}
		// Duplicate -> advance to discard
		mi.stats.TotalDuplicates++
		hasNext := itsi.Next()
		if err := its0.Error(); err != nil {
			mi.errs = append(mi.errs, err)
		}
		if !hasNext {
			// stream finished: remove it
			itsi.Close()
			itsi.SampleIterator = nil
			mi.iActiveIts = slices.Delete(mi.iActiveIts, i, i+1)
			// restart iteration from the same position
			i--
			continue
		}
		*itsi.Sample = itsi.SampleIterator.Sample()
		itsi.labels = itsi.Labels()
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
	for i := range mi.iActiveIts {
		itsi := &mi.its[mi.iActiveIts[i]]
		if s, ok := itsi.SampleIterator.(Seekable); ok {
			hasNext := s.Seek(t)
			if err := itsi.Error(); err != nil {
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
			itsi.labels = itsi.Labels()
			mi.iActiveIts[j] = mi.iActiveIts[i]
			j++
		} else {
			// Seek not supported: use Next() to advance
			var advanced bool
			for itsi.Timestamp < t {
				hasNext := itsi.Next()
				if err := itsi.Error(); err != nil {
					mi.errs = append(mi.errs, err)
				}
				if !hasNext {
					// stream finished: remove it
					itsi.Close()
					itsi.SampleIterator = nil
					break
				}
				*itsi.Sample = itsi.SampleIterator.Sample()
				itsi.labels = itsi.Labels()
				advanced = true
			}
			if itsi.SampleIterator != nil {
				mi.iActiveIts[j] = mi.iActiveIts[i]
				if advanced {
					needSort = true
					*itsi.Sample = itsi.SampleIterator.Sample()
					itsi.labels = itsi.Labels()
				}
				j++
			}
		}
	}
	mi.iActiveIts = mi.iActiveIts[:j]
	if len(mi.iActiveIts) == 0 {
		mi.curSample, mi.curLabels = model.Sample{}, ""
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
	Recv() (*model.SampleQueryResponse, error)
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

func (i *sampleQueryClientIterator) Sample() model.Sample {
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
func NewSampleQueryResponseIterator(
	ctx context.Context,
	resp *model.SampleQueryResponse,
) SampleIterator {
	return NewMultiSeriesIterator(ctx, resp.Series)
}

type seriesIterator struct {
	i       int
	samples []model.Sample
	labels  string
}

// NewMultiSeriesIterator returns an iterator over multiple logproto.Series
func NewMultiSeriesIterator(ctx context.Context, series []model.Series) SampleIterator {
	is := make([]SampleIterator, 0, len(series))
	for i := range series {
		is = append(is, NewSeriesIterator(series[i]))
	}
	return NewHeapSampleIterator(ctx, is)
}

// NewSeriesIterator iterates over sample in a series.
func NewSeriesIterator(series model.Series) SampleIterator {
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

func (i *seriesIterator) Sample() model.Sample {
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

func (i *nonOverlappingSampleIterator) Sample() model.Sample {
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
		i.Close()
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
		i.Close()
	}
	return ok
}

// ReadBatch reads a set of entries off an iterator.
func ReadSampleBatch(i SampleIterator, size uint32) (*model.SampleQueryResponse, uint32, error) {
	series := map[string]*model.Series{}
	respSize := uint32(0)
	for ; respSize < size && i.Next(); respSize++ {
		labels, sample := i.Labels(), i.Sample()
		s, ok := series[labels]
		if !ok {
			s = &model.Series{
				Labels: labels,
			}
			series[labels] = s
		}
		s.Samples = append(s.Samples, sample)
	}

	result := model.SampleQueryResponse{
		Series: make([]model.Series, 0, len(series)),
	}
	for _, s := range series {
		result.Series = append(result.Series, *s)
	}
	return &result, respSize, i.Error()
}
