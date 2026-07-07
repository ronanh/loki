package log

import (
	"bytes"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
)

const MaxInternedStrings = 1024

var emptyLabelsResult = NewLabelsResult(
	labels.EmptyLabels(),
	labels.StableHash(labels.EmptyLabels()),
)

// LabelsResult is a computed labels result that contains the labels set with associated string and
// hash.
// The is mainly used for caching and returning labels computations out of pipelines and stages.
//
// Results are categorized (deltaLabels design, xlog line-attrs study D2.1):
// Stream/Delta/Parsed return the per-category subsets of Labels(), and
// Deleted returns the stream label names removed by the pipeline (drop/del).
// The categorized view is what the store needs to encode an output delta
// relative to the stream labels without any set-diffing.
type LabelsResult interface {
	String() string
	Labels() labels.Labels
	Hash() uint64
	// Stream returns the labels of Labels() that come from the stream.
	Stream() labels.Labels
	// Delta returns the labels of Labels() that come from the per-line
	// deltaLabels (after any query-time collision rename).
	Delta() labels.Labels
	// Parsed returns the labels of Labels() produced by pipeline stages
	// (parsers, label_format, ...; includes __error__ when set).
	Parsed() labels.Labels
	// Deleted returns the stream label names removed by the pipeline.
	Deleted() []string
}

// NewLabelsResult creates a new LabelsResult from a labels set and a hash.
// The whole set is categorized as stream labels.
func NewLabelsResult(lbs labels.Labels, hash uint64) LabelsResult {
	return &labelsResult{lbs: lbs, s: labelsString(lbs), h: hash, stream: lbs}
}

// NewCategorizedLabelsResult creates a LabelsResult carrying the per-category
// decomposition of all (stream ⊎ delta ⊎ parsed = all, deleted = stream label
// names removed by the pipeline).
func NewCategorizedLabelsResult(
	all labels.Labels,
	hash uint64,
	stream, delta, parsed labels.Labels,
	deleted []string,
) LabelsResult {
	return &labelsResult{
		lbs: all, s: labelsString(all), h: hash,
		stream: stream, delta: delta, parsed: parsed, deleted: deleted,
	}
}

func labelsString(ls labels.Labels) string {
	var b bytes.Buffer
	size := 2
	ls.Range(func(l labels.Label) {
		size += len(l.Name) + len(l.Value) + 5
	})
	b.Grow(size)

	b.WriteByte('{')
	i := 0
	ls.Range(func(l labels.Label) {
		if i > 0 {
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		bytesBufferQuoteTo(&b, l.Value)
		i++
	})
	b.WriteByte('}')
	return b.String()
}

const (
	lowerhex = "0123456789abcdef"
)

// bytesBufferQuoteTo writes a quoted string to a bytes.Buffer
// heavily inspired from GO strconv/quote.go
// https://cs.opensource.google/go/go/+/refs/tags/go1.21.5:LICENSE
func bytesBufferQuoteTo(b *bytes.Buffer, s string) {
	_ = b.WriteByte('"')
	for i, r := range s {
		if 0x20 <= r && r <= 0x7E && r != '\\' && r != '"' {
			// fast path for common case
			_ = b.WriteByte(byte(r))
			continue
		}

		width := 1
		if r >= utf8.RuneSelf {
			width = utf8.RuneLen(r)
		}
		if r == utf8.RuneError && width == 1 {
			_, _ = b.WriteString(`\x`)
			_ = b.WriteByte(lowerhex[s[i]>>4])
			_ = b.WriteByte(lowerhex[s[i]&0xF])
			continue
		}
		{
			var runeTmp [utf8.UTFMax]byte
			if r == '"' || r == '\\' {
				_ = b.WriteByte('\\')
				_ = b.WriteByte('"')
				continue
			}
			if strconv.IsPrint(r) {
				n := utf8.EncodeRune(runeTmp[:], r)
				_, _ = b.Write(runeTmp[:n])
				continue
			}
			switch r {
			case '\a':
				_, _ = b.WriteString(`\a`)
			case '\b':
				_, _ = b.WriteString(`\b`)
			case '\f':
				_, _ = b.WriteString(`\f`)
			case '\n':
				_, _ = b.WriteString(`\n`)
			case '\r':
				_, _ = b.WriteString(`\r`)
			case '\t':
				_, _ = b.WriteString(`\t`)
			case '\v':
				_, _ = b.WriteString(`\v`)
			default:
				switch {
				case r < ' ' || r == 0x7f:
					_, _ = b.WriteString(`\x`)
					_ = b.WriteByte(lowerhex[byte(r)>>4])
					_ = b.WriteByte(lowerhex[byte(r)&0xF])
				case !utf8.ValidRune(r):
					r = 0xFFFD
					fallthrough
				case r < 0x10000:
					_, _ = b.WriteString(`\u`)
					for s := 12; s >= 0; s -= 4 {
						_ = b.WriteByte(lowerhex[r>>uint(s)&0xF])
					}
				default:
					_, _ = b.WriteString(`\U`)
					for s := 28; s >= 0; s -= 4 {
						_ = b.WriteByte(lowerhex[r>>uint(s)&0xF])
					}
				}
			}
		}
	}
	_ = b.WriteByte('"')
}

type labelsResult struct {
	lbs labels.Labels
	s   string
	h   uint64

	stream, delta, parsed labels.Labels
	deleted               []string
}

func (l labelsResult) String() string {
	return l.s
}

func (l labelsResult) Labels() labels.Labels {
	return l.lbs
}

func (l labelsResult) Hash() uint64 {
	return l.h
}

func (l labelsResult) Stream() labels.Labels {
	return l.stream
}

func (l labelsResult) Delta() labels.Labels {
	return l.delta
}

func (l labelsResult) Parsed() labels.Labels {
	return l.parsed
}

func (l labelsResult) Deleted() []string {
	return l.deleted
}

var seps = []byte{'\xff'}

type hasher struct {
	buf []byte // buffer for computing hash without bytes slice allocation.
}

// newHasher allow to compute hashes for labels by reusing the same buffer.
func newHasher() *hasher {
	return &hasher{
		buf: make([]byte, 0, 1024),
	}
}

// Hash hashes the labels
func (h *hasher) Hash(lbs labels.Labels) uint64 {
	var hash uint64
	hash, h.buf = lbs.HashWithoutLabels(h.buf, []string(nil)...)
	return hash
}

// hash hashes a scratch labels slice with the same algorithm as
// labels.Labels.HashWithoutLabels (name \xff value \xff, skipping __name__) so
// that cache keys are consistent with Hash whenever the slice is sorted.
func (h *hasher) hash(lbs []labels.Label) uint64 {
	b := h.buf[:0]
	for _, l := range lbs {
		if l.Name == labels.MetricName {
			continue
		}
		b = append(b, l.Name...)
		b = append(b, seps[0])
		b = append(b, l.Value...)
		b = append(b, seps[0])
	}
	h.buf = b
	return xxhash.Sum64(b)
}

// BaseLabelsBuilder is a label builder used by pipeline and stages.
// Only one base builder is used and it contains cache for each LabelsBuilders.
type BaseLabelsBuilder struct {
	del []string
	add []labels.Label
	// deltas holds the per-line deltaLabels injected by Process (after any
	// query-time collision rename against the stream labels); reset per line.
	deltas []labels.Label
	// deltaHash is the caller-provided canonical hash of the line's effective
	// label set (stream ⊕ delta), 0 = unavailable/recompute. Only trusted on
	// the fast path where no stage touched the labels.
	deltaHash uint64
	// nolint(structcheck) https://github.com/golangci/golangci-lint/issues/826
	err string

	groups            []string
	parserKeyHints    ParserHint // label key hints for metric queries that allows to limit parser extractions to only this list of labels.
	without, noLabels bool

	resultCache map[uint64]LabelsResult
	*hasher
}

// LabelsBuilder is the same as labels.Builder but tailored for this package.
type LabelsBuilder struct {
	base          labels.Labels
	baseMap       map[string]string
	buf           []labels.Label
	currentResult LabelsResult
	groupedResult LabelsResult

	*BaseLabelsBuilder
}

// NewBaseLabelsBuilderWithGrouping creates a new base labels builder with grouping to compute
// results.
func NewBaseLabelsBuilderWithGrouping(
	groups []string,
	parserKeyHints ParserHint,
	without, noLabels bool,
) *BaseLabelsBuilder {
	return &BaseLabelsBuilder{
		// del:            make([]string, 0, 5),
		// add:            make([]labels.Label, 0, 16),
		// resultCache:    make(map[uint64]LabelsResult),
		// hasher:         newHasher(),
		groups:         groups,
		parserKeyHints: parserKeyHints,
		noLabels:       noLabels,
		without:        without,
	}
}

// NewLabelsBuilder creates a new base labels builder.
func NewBaseLabelsBuilder() *BaseLabelsBuilder {
	return NewBaseLabelsBuilderWithGrouping(nil, noParserHints, false, false)
}

// ForLabels creates a labels builder for a given labels set as base.
// The labels cache is shared across all created LabelsBuilders.
func (b *BaseLabelsBuilder) ForLabels(lbs labels.Labels, hash uint64) *LabelsBuilder {
	if labelResult, ok := b.resultCache[hash]; ok {
		res := &LabelsBuilder{
			base:              lbs,
			currentResult:     labelResult,
			BaseLabelsBuilder: b,
		}
		return res
	}
	labelResult := NewLabelsResult(lbs, hash)
	if b.resultCache == nil {
		b.resultCache = make(map[uint64]LabelsResult, 1)
	}
	b.resultCache[hash] = labelResult
	res := &LabelsBuilder{
		base:              lbs,
		currentResult:     labelResult,
		BaseLabelsBuilder: b,
	}
	return res
}

// Reset clears all current state for the builder.
func (b *LabelsBuilder) Reset() {
	b.del = b.del[:0]
	b.add = b.add[:0]
	b.deltas = b.deltas[:0]
	b.deltaHash = 0
	b.err = ""
}

// BaseLabels returns the stream's base labels result (unaffected by the
// current line's state).
func (b *LabelsBuilder) BaseLabels() LabelsResult {
	return b.currentResult
}

// SetDeltaLabels injects the current line's deltaLabels (additive per-line
// labels relative to the stream labels, decoded from storage) with category
// DeltaLabel. A delta name colliding with a stream label is renamed
// name_extracted (query-time convention — ingest prevents this for data we
// wrote, so the rename is the foreign/reprocessed-data slow path and forces
// hash recomputation). deltaHash is the stored canonical hash of
// (stream ⊕ delta), 0 = recompute.
//
// Must be called right after Reset and before any stage runs.
func (b *LabelsBuilder) SetDeltaLabels(deltaLabels labels.Labels, deltaHash uint64) {
	if deltaLabels.IsEmpty() {
		return
	}
	forceRecompute := false
	deltaLabels.Range(func(l labels.Label) {
		if l.Value == "" {
			// empty-valued delta labels do not exist by the frozen format
			// rules (SET "" is written as DEL); tolerate foreign input by
			// ignoring the entry and distrusting the stored hash
			forceRecompute = true
			return
		}
		name := l.Name
		if b.base.Has(name) {
			name += duplicateSuffix
			forceRecompute = true
		}
		b.deltas = append(b.deltas, labels.Label{Name: name, Value: l.Value})
	})
	if forceRecompute {
		deltaHash = 0
	}
	b.deltaHash = deltaHash
}

// hasDeltas reports whether the current line carries delta labels.
func (b *BaseLabelsBuilder) hasDeltas() bool {
	return len(b.deltas) > 0
}

func (b *BaseLabelsBuilder) Hash(lbs labels.Labels) uint64 {
	if b.hasher == nil {
		b.hasher = newHasher()
	}
	return b.hasher.Hash(lbs)
}

// hashScratch hashes a scratch labels slice, sharing the hasher buffer.
func (b *BaseLabelsBuilder) hashScratch(lbs []labels.Label) uint64 {
	if b.hasher == nil {
		b.hasher = newHasher()
	}
	return b.hasher.hash(lbs)
}

// mixHash combines two 64-bit hashes into a cache key (order-sensitive).
func (b *BaseLabelsBuilder) mixHash(a, h uint64) uint64 {
	if b.hasher == nil {
		b.hasher = newHasher()
	}
	buf := append(b.hasher.buf[:0],
		byte(a), byte(a>>8), byte(a>>16), byte(a>>24),
		byte(a>>32), byte(a>>40), byte(a>>48), byte(a>>56),
		byte(h), byte(h>>8), byte(h>>16), byte(h>>24),
		byte(h>>32), byte(h>>40), byte(h>>48), byte(h>>56))
	b.hasher.buf = buf
	return xxhash.Sum64(buf)
}

// categorizedKey computes a result-cache key covering the full categorized
// builder state: the stream hash (seed) plus the delta labels, parsed labels,
// deleted names and error, each category separated by \xfe. Unlike the
// effective-set hash, equal effective sets with different category
// decompositions get distinct keys.
func (b *BaseLabelsBuilder) categorizedKey(baseHash uint64) uint64 {
	if b.hasher == nil {
		b.hasher = newHasher()
	}
	buf := append(b.hasher.buf[:0],
		byte(baseHash), byte(baseHash>>8), byte(baseHash>>16), byte(baseHash>>24),
		byte(baseHash>>32), byte(baseHash>>40), byte(baseHash>>48), byte(baseHash>>56))
	for _, l := range b.deltas {
		buf = append(buf, l.Name...)
		buf = append(buf, seps[0])
		buf = append(buf, l.Value...)
		buf = append(buf, seps[0])
	}
	buf = append(buf, '\xfe')
	for _, l := range b.add {
		buf = append(buf, l.Name...)
		buf = append(buf, seps[0])
		buf = append(buf, l.Value...)
		buf = append(buf, seps[0])
	}
	buf = append(buf, '\xfe')
	for _, n := range b.del {
		buf = append(buf, n...)
		buf = append(buf, seps[0])
	}
	buf = append(buf, '\xfe')
	buf = append(buf, b.err...)
	b.hasher.buf = buf
	return xxhash.Sum64(buf)
}

// ParserLabelHints returns a limited list of expected labels to extract for metric queries.
// Returns nil when it's impossible to hint labels extractions.
func (b *BaseLabelsBuilder) ParserLabelHints() ParserHint {
	return b.parserKeyHints
}

// SetErr sets the error label.
func (b *LabelsBuilder) SetErr(err string) *LabelsBuilder {
	b.err = err
	return b
}

// GetErr return the current error label value.
func (b *LabelsBuilder) GetErr() string {
	return b.err
}

// HasErr tells if the error label has been set.
func (b *LabelsBuilder) HasErr() bool {
	return b.err != ""
}

// BaseHas returns the base labels have the given key
func (b *LabelsBuilder) BaseHas(key string) bool {
	return b.base.Has(key)
}

// Get returns the value of a labels key if it exists.
// Precedence: parsed (stage Sets) > deleted > delta labels > stream labels.
func (b *LabelsBuilder) Get(key string) (string, bool) {
	for _, a := range b.add {
		if a.Name == key {
			return a.Value, true
		}
	}
	if slices.Contains(b.del, key) {
		return "", false
	}
	for _, d := range b.deltas {
		if d.Name == key {
			return d.Value, true
		}
	}

	if v := b.base.Get(key); v != "" {
		return v, true
	}
	return "", false
}

// Del deletes the label of the given name.
func (b *LabelsBuilder) Del(ns ...string) *LabelsBuilder {
	for _, n := range ns {
		for i, a := range b.add {
			if a.Name == n {
				b.add = append(b.add[:i], b.add[i+1:]...)
			}
		}
		b.del = append(b.del, n)
	}
	return b
}

// Set the name/value pair as a label. A value of "" means delete that label,
// mirroring labels.Builder semantics: empty-valued labels do not exist.
func (b *LabelsBuilder) Set(n, v string) *LabelsBuilder {
	if v == "" {
		return b.Del(n)
	}
	for i, a := range b.add {
		if a.Name == n {
			b.add[i].Value = v
			return b
		}
	}
	b.add = append(b.add, labels.Label{Name: n, Value: v})

	return b
}

// labels returns the labels from the builder, sorted, as a reusable scratch
// slice. If no modifications were made, the base labels are returned.
func (b *LabelsBuilder) labels() []labels.Label {
	b.buf = b.unsortedLabels(b.buf)
	slices.SortFunc(b.buf, func(a, b labels.Label) int { return strings.Compare(a.Name, b.Name) })
	return b.buf
}

func (b *LabelsBuilder) unsortedLabels(buf []labels.Label) []labels.Label {
	if len(b.del) == 0 && len(b.add) == 0 && len(b.deltas) == 0 {
		if buf == nil {
			buf = make([]labels.Label, 0, b.base.Len()+1)
		} else {
			buf = buf[:0]
		}
		b.base.Range(func(l labels.Label) {
			buf = append(buf, l)
		})
		if b.err != "" {
			buf = append(buf, labels.Label{Name: ErrorLabel, Value: b.err})
		}
		return buf
	}

	// In the general case, labels are removed, modified or moved
	// rather than added.
	if buf == nil {
		buf = make([]labels.Label, 0, b.base.Len()+len(b.deltas)+len(b.add)+1)
	} else {
		buf = buf[:0]
	}
	b.base.Range(func(l labels.Label) {
		for _, n := range b.del {
			if l.Name == n {
				return
			}
		}
		for _, la := range b.add {
			if l.Name == la.Name {
				return
			}
		}
		for _, ld := range b.deltas {
			// only possible on the rename-once edge (base holds both name
			// and name_extracted); deltas otherwise never collide with base
			if l.Name == ld.Name {
				return
			}
		}
		buf = append(buf, l)
	})
	for _, ld := range b.deltas {
		if slices.Contains(b.del, ld.Name) {
			continue
		}
		if labelsContainName(b.add, ld.Name) {
			// parsed labels win over delta labels
			continue
		}
		buf = append(buf, ld)
	}
	buf = append(buf, b.add...)
	if b.err != "" {
		buf = append(buf, labels.Label{Name: ErrorLabel, Value: b.err})
	}
	return buf
}

func labelsContainName(lbs []labels.Label, name string) bool {
	for _, l := range lbs {
		if l.Name == name {
			return true
		}
	}
	return false
}

type stringMapPool struct {
	pool sync.Pool
}

func newStringMapPool() *stringMapPool {
	return &stringMapPool{
		pool: sync.Pool{
			New: func() any {
				return make(map[string]string)
			},
		},
	}
}

func (s *stringMapPool) Get() map[string]string {
	m := s.pool.Get().(map[string]string)
	return m
}

func (s *stringMapPool) Put(m map[string]string) {
	clear(m)
	s.pool.Put(m)
}

var smp = newStringMapPool()

// puts labels entries into an existing map, it is up to the caller to
// properly clear the map if it is going to be reused
func (b *LabelsBuilder) IntoMap(m map[string]string) {
	if len(b.del) == 0 && len(b.add) == 0 && len(b.deltas) == 0 && !b.HasErr() {
		if b.baseMap == nil {
			b.baseMap = b.base.Map()
			maps.Copy(m, b.baseMap)
		}
		return
	}
	b.buf = b.unsortedLabels(b.buf)
	// todo should we also cache maps since limited by the result ?
	// Maps also don't create a copy of the labels.
	for _, l := range b.buf {
		m[l.Name] = l.Value
	}
}

func (b *LabelsBuilder) Map() (map[string]string, bool) {
	if len(b.del) == 0 && len(b.add) == 0 && len(b.deltas) == 0 && b.err == "" {
		if b.baseMap == nil {
			b.baseMap = b.base.Map()
		}
		return b.baseMap, false
	}
	b.buf = b.unsortedLabels(b.buf)
	res := smp.Get()
	for _, l := range b.buf {
		res[l.Name] = l.Value
	}
	return res, true
}

// LabelsResult returns the LabelsResult from the builder.
// No grouping is applied and the cache is used when possible.
func (b *LabelsBuilder) LabelsResult() LabelsResult {
	// unchanged path.
	if len(b.del) == 0 && len(b.add) == 0 && b.err == "" {
		if len(b.deltas) == 0 {
			return b.currentResult
		}
		return b.deltaResult()
	}
	return b.toCategorizedResult()
}

// deltaResult is the hot path for a line whose only label state is its delta
// labels (no stage touched labels): when the stored deltaHash is available it
// is used directly — as the cache key (mixed with the stream hash so equal
// effective sets with different stream/delta splits never alias) and as the
// result Hash() — so repeated (stream, delta) combinations do no hashing and
// no label materialization at all.
func (b *LabelsBuilder) deltaResult() LabelsResult {
	if b.deltaHash == 0 {
		return b.toCategorizedResult()
	}
	key := b.mixHash(b.currentResult.Hash(), b.deltaHash)
	if cached, ok := b.resultCache[key]; ok {
		return cached
	}
	res := b.newCategorizedResult(b.deltaHash)
	if b.resultCache == nil {
		b.resultCache = make(map[uint64]LabelsResult, 1)
	}
	b.resultCache[key] = res
	return res
}

// toCategorizedResult builds (or fetches from cache) the categorized result
// for the current builder state. The cache key covers the per-category
// content (seeded with the stream hash), NOT just the effective set: two
// lines with equal effective labels but different stream/delta/parsed
// decompositions must not share a categorized result.
func (b *LabelsBuilder) toCategorizedResult() LabelsResult {
	key := b.categorizedKey(b.currentResult.Hash())
	if cached, ok := b.resultCache[key]; ok {
		return cached
	}
	res := b.newCategorizedResult(0)
	if b.resultCache == nil {
		b.resultCache = make(map[uint64]LabelsResult, 1)
	}
	b.resultCache[key] = res
	return res
}

// newCategorizedResult materializes the current builder state into a
// categorized LabelsResult. knownHash, when non-zero, is trusted as the
// stable hash of the effective set (stored deltaHash fast path); otherwise
// the hash is computed (same algorithm/values as before deltas existed).
func (b *LabelsBuilder) newCategorizedResult(knownHash uint64) LabelsResult {
	buf := b.labels()
	hash := knownHash
	if hash == 0 {
		hash = b.hashScratch(buf)
	}
	all := labels.New(buf...)

	// stream = base minus deleted minus shadowed-by-parsed (deltas cannot
	// shadow base post-rename, modulo the rename-once edge handled below)
	streamB := labels.NewScratchBuilder(b.base.Len())
	var deleted []string
	b.base.Range(func(l labels.Label) {
		if slices.Contains(b.del, l.Name) {
			deleted = append(deleted, l.Name)
			return
		}
		if labelsContainName(b.add, l.Name) || labelsContainName(b.deltas, l.Name) {
			return
		}
		streamB.Add(l.Name, l.Value)
	})
	stream := streamB.Labels()

	delta := labels.EmptyLabels()
	if len(b.deltas) > 0 {
		deltaB := labels.NewScratchBuilder(len(b.deltas))
		for _, ld := range b.deltas {
			if slices.Contains(b.del, ld.Name) || labelsContainName(b.add, ld.Name) {
				continue
			}
			deltaB.Add(ld.Name, ld.Value)
		}
		deltaB.Sort()
		delta = deltaB.Labels()
	}

	parsed := labels.EmptyLabels()
	if len(b.add) > 0 || b.err != "" {
		parsedB := labels.NewScratchBuilder(len(b.add) + 1)
		for _, la := range b.add {
			parsedB.Add(la.Name, la.Value)
		}
		if b.err != "" {
			parsedB.Add(ErrorLabel, b.err)
		}
		parsedB.Sort()
		parsed = parsedB.Labels()
	}

	return NewCategorizedLabelsResult(all, hash, stream, delta, parsed, deleted)
}

// toResult caches a plain (uncategorized) result keyed by the content hash of
// buf. Used by the grouped paths, whose outputs are aggregation-facing and
// carry no categorization.
func (b *BaseLabelsBuilder) toResult(buf []labels.Label) LabelsResult {
	hash := b.hashScratch(buf)
	if cached, ok := b.resultCache[hash]; ok {
		return cached
	}
	res := NewLabelsResult(labels.New(buf...), hash)
	if b.resultCache == nil {
		b.resultCache = make(map[uint64]LabelsResult, 1)
	}
	b.resultCache[hash] = res
	return res
}

// GroupedLabels returns the LabelsResult from the builder.
// Groups are applied and the cache is used when possible.
func (b *LabelsBuilder) GroupedLabels() LabelsResult {
	if b.err != "" {
		// We need to return now before applying grouping otherwise the error might get lost.
		return b.LabelsResult()
	}
	if b.noLabels {
		return emptyLabelsResult
	}
	// unchanged path (delta labels count as changes: they must flow into the
	// grouped output).
	if len(b.del) == 0 && len(b.add) == 0 && len(b.deltas) == 0 {
		if len(b.groups) == 0 {
			return b.currentResult
		}
		return b.toBaseGroup()
	}
	// no grouping
	if len(b.groups) == 0 {
		return b.LabelsResult()
	}

	if b.without {
		return b.withoutResult()
	}
	return b.withResult()
}

func (b *LabelsBuilder) withResult() LabelsResult {
	if b.buf == nil {
		b.buf = make([]labels.Label, 0, len(b.groups))
	} else {
		b.buf = b.buf[:0]
	}
Outer:
	for _, g := range b.groups {
		for _, n := range b.del {
			if g == n {
				continue Outer
			}
		}
		for _, la := range b.add {
			if g == la.Name {
				b.buf = append(b.buf, la)
				continue Outer
			}
		}
		for _, ld := range b.deltas {
			if g == ld.Name {
				b.buf = append(b.buf, ld)
				continue Outer
			}
		}
		if v := b.base.Get(g); v != "" {
			b.buf = append(b.buf, labels.Label{Name: g, Value: v})
		}
	}
	return b.toResult(b.buf)
}

func (b *LabelsBuilder) withoutResult() LabelsResult {
	if b.buf == nil {
		size := max(b.base.Len()+len(b.add)-len(b.del)-len(b.groups), 0)
		b.buf = make([]labels.Label, 0, size)
	} else {
		b.buf = b.buf[:0]
	}
	b.base.Range(func(l labels.Label) {
		for _, n := range b.del {
			if l.Name == n {
				return
			}
		}
		for _, la := range b.add {
			if l.Name == la.Name {
				return
			}
		}
		for _, ld := range b.deltas {
			if l.Name == ld.Name {
				return
			}
		}
		for _, lg := range b.groups {
			if l.Name == lg {
				return
			}
		}
		b.buf = append(b.buf, l)
	})
OuterDelta:
	for _, ld := range b.deltas {
		if slices.Contains(b.del, ld.Name) || labelsContainName(b.add, ld.Name) {
			continue
		}
		for _, lg := range b.groups {
			if ld.Name == lg {
				continue OuterDelta
			}
		}
		b.buf = append(b.buf, ld)
	}
OuterAdd:
	for _, la := range b.add {
		for _, lg := range b.groups {
			if la.Name == lg {
				continue OuterAdd
			}
		}
		b.buf = append(b.buf, la)
	}
	slices.SortFunc(b.buf, func(a, b labels.Label) int { return strings.Compare(a.Name, b.Name) })
	return b.toResult(b.buf)
}

func (b *LabelsBuilder) toBaseGroup() LabelsResult {
	if b.groupedResult != nil {
		return b.groupedResult
	}
	var lbs labels.Labels
	if b.without {
		// Del(MetricName) preserves the historic WithoutLabels behavior which
		// always dropped __name__.
		lbs = labels.NewBuilder(b.base).Del(b.groups...).Del(labels.MetricName).Labels()
	} else {
		lbs = labels.NewBuilder(b.base).Keep(b.groups...).Labels()
	}
	res := NewLabelsResult(lbs, labels.StableHash(lbs))
	b.groupedResult = res
	return res
}

type internedStringSet map[string]struct {
	s  string
	ok bool
}

func (i internedStringSet) Get(data []byte, createNew func() (string, bool)) (string, bool) {
	s, ok := i[string(data)]
	if ok {
		return s.s, s.ok
	}
	new, ok := createNew()
	if len(i) >= MaxInternedStrings {
		return new, ok
	}
	i[string(data)] = struct {
		s  string
		ok bool
	}{s: new, ok: ok}
	return new, ok
}
