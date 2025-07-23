package log

import (
	"bytes"
	"sort"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/prometheus/prometheus/pkg/labels"
)

const MaxInternedStrings = 1024

var emptyLabelsResult = NewLabelsResult(labels.Labels{}, labels.Labels{}.Hash())

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
	for _, l := range ls {
		size += len(l.Name) + len(l.Value) + 5
	}
	b.Grow(size)

	b.WriteByte('{')
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		bytesBufferQuoteTo(&b, l.Value)
		// b.WriteString(strconv.Quote(l.Value))
	}
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

type hasher struct {
	buf []byte // buffer for computing hash without bytes slice allocation.
}

// newHasher allow to compute hashes for labels by reusing the same buffer.
func newHasher() *hasher {
	return &hasher{
		buf: make([]byte, 0, 1024),
	}
}

// Hash hashes the labels.
func (h *hasher) Hash(lbs labels.Labels) uint64 {
	var hash uint64
	hash, h.buf = lbs.HashWithoutLabels(h.buf, []string(nil)...)
	return hash
}

// BaseLabelsBuilder is a label builder used by pipeline and stages.
// Only one base builder is used and it contains cache for each LabelsBuilders.
type BaseLabelsBuilder struct {
	del []string
	add []labels.Label
	//nolint(structcheck) https://github.com/golangci/golangci-lint/issues/826
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
	buf           labels.Labels
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

// BaseHas returns the base labels have the given key.
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
	for _, d := range b.del {
		if d == key {
			return "", false
		}
	}

	for _, l := range b.base {
		if l.Name == key {
			return l.Value, true
		}
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

// Set the name/value pair as a label.
func (b *LabelsBuilder) Set(n, v string) *LabelsBuilder {
	for i, a := range b.add {
		if a.Name == n {
			b.add[i].Value = v
			return b
		}
	}
	b.add = append(b.add, labels.Label{Name: n, Value: v})

	return b
}

// Labels returns the labels from the builder. If no modifications
// were made, the original labels are returned.
func (b *LabelsBuilder) labels() labels.Labels {
	b.buf = b.unsortedLabels(b.buf)
	sort.Sort(b.buf)
	return b.buf
}

func (b *LabelsBuilder) unsortedLabels(buf labels.Labels) labels.Labels {
	if len(b.del) == 0 && len(b.add) == 0 {
		if buf == nil {
			buf = make(labels.Labels, 0, len(b.base)+1)
		} else {
			buf = buf[:0]
		}
		buf = append(buf, b.base...)
		if b.err != "" {
			buf = append(buf, labels.Label{Name: ErrorLabel, Value: b.err})
		}
		return buf
	}

	// In the general case, labels are removed, modified or moved
	// rather than added.
	if buf == nil {
		buf = make(labels.Labels, 0, len(b.base)+len(b.add)+1)
	} else {
		buf = buf[:0]
	}
Outer:
	for _, l := range b.base {
		for _, n := range b.del {
			if l.Name == n {
				continue Outer
			}
		}
		for _, la := range b.add {
			if l.Name == la.Name {
				continue Outer
			}
		}
		buf = append(buf, l)
	}
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
			New: func() interface{} {
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
// properly clear the map if it is going to be reused.
func (b *LabelsBuilder) IntoMap(m map[string]string) {
	if len(b.del) == 0 && len(b.add) == 0 && !b.HasErr() {
		if b.baseMap == nil {
			b.baseMap = b.base.Map()
			for k, v := range b.baseMap {
				m[k] = v
			}
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

func (b *BaseLabelsBuilder) toResult(buf labels.Labels) LabelsResult {
	hash := b.Hash(buf)
	if cached, ok := b.resultCache[hash]; ok {
		return cached
	}
	res := NewLabelsResult(buf.Copy(), hash)
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
		b.buf = make(labels.Labels, 0, len(b.groups))
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
		for _, l := range b.base {
			if g == l.Name {
				b.buf = append(b.buf, l)
				continue Outer
			}
		}
	}
	return b.toResult(b.buf)
}

func (b *LabelsBuilder) withoutResult() LabelsResult {
	if b.buf == nil {
		size := len(b.base) + len(b.add) - len(b.del) - len(b.groups)
		if size < 0 {
			size = 0
		}
		b.buf = make(labels.Labels, 0, size)
	} else {
		b.buf = b.buf[:0]
	}
Outer:
	for _, l := range b.base {
		for _, n := range b.del {
			if l.Name == n {
				continue Outer
			}
		}
		for _, la := range b.add {
			if l.Name == la.Name {
				continue Outer
			}
		}
		for _, lg := range b.groups {
			if l.Name == lg {
				continue Outer
			}
		}
		b.buf = append(b.buf, l)
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
	sort.Sort(b.buf)
	return b.toResult(b.buf)
}

func (b *LabelsBuilder) toBaseGroup() LabelsResult {
	if b.groupedResult != nil {
		return b.groupedResult
	}
	var lbs labels.Labels
	if b.without {
		lbs = b.base.WithoutLabels(b.groups...)
	} else {
		lbs = b.base.WithLabels(b.groups...)
	}
	res := NewLabelsResult(lbs, lbs.Hash())
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
