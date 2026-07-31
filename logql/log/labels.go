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
type LabelsResult interface {
	String() string
	Labels() labels.Labels
	Hash() uint64
}

// NewLabelsResult creates a new LabelsResult from a labels set and a hash.
func NewLabelsResult(lbs labels.Labels, hash uint64) LabelsResult {
	return &labelsResult{lbs: lbs, s: labelsString(lbs), h: hash}
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
	b.err = ""
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
func (b *LabelsBuilder) Get(key string) (string, bool) {
	for _, a := range b.add {
		if a.Name == key {
			return a.Value, true
		}
	}
	if slices.Contains(b.del, key) {
		return "", false
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
	if len(b.del) == 0 && len(b.add) == 0 {
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
		buf = make([]labels.Label, 0, b.base.Len()+len(b.add)+1)
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
		buf = append(buf, l)
	})
	buf = append(buf, b.add...)
	if b.err != "" {
		buf = append(buf, labels.Label{Name: ErrorLabel, Value: b.err})
	}
	return buf
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
	if len(b.del) == 0 && len(b.add) == 0 && !b.HasErr() {
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
	if len(b.del) == 0 && len(b.add) == 0 && b.err == "" {
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
		return b.currentResult
	}
	return b.toResult(b.labels())
}

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
	// unchanged path.
	if len(b.del) == 0 && len(b.add) == 0 {
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
		for _, lg := range b.groups {
			if l.Name == lg {
				return
			}
		}
		b.buf = append(b.buf, l)
	})
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

// toBaseGroup groups the base labels, for the case where no stage added or
// removed a label. The result is cached per stream in b.groupedResult (base
// and groups are both immutable for the lifetime of a LabelsBuilder, so Reset
// does not have to invalidate it).
//
// It filters into the shared b.buf scratch slice and goes through toResult,
// exactly like the withResult/withoutResult siblings: that keeps the grouped
// result deduplicated across streams via the base builder's resultCache — a
// metric query grouping thousands of streams down to a handful of distinct
// label sets then retains a handful of LabelsResult, not one per stream. Under
// stringlabels that matters more than it did with []Label: every materialized
// labels.Labels owns a full private copy of its name/value bytes, where the
// pre-migration slice-of-Label result shared its strings with the base.
//
// b.base is sorted, and filtering preserves order, so no sort is needed here.
func (b *LabelsBuilder) toBaseGroup() LabelsResult {
	if b.groupedResult != nil {
		return b.groupedResult
	}
	if b.buf == nil {
		b.buf = make([]labels.Label, 0, b.base.Len())
	} else {
		b.buf = b.buf[:0]
	}
	if b.without {
		// __name__ is dropped unconditionally, preserving the historic
		// WithoutLabels behaviour.
		b.base.Range(func(l labels.Label) {
			if l.Name == labels.MetricName {
				return
			}
			for _, g := range b.groups {
				if l.Name == g {
					return
				}
			}
			b.buf = append(b.buf, l)
		})
	} else {
		b.base.Range(func(l labels.Label) {
			for _, g := range b.groups {
				if l.Name == g {
					b.buf = append(b.buf, l)
					return
				}
			}
		})
	}
	res := b.toResult(b.buf)
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
