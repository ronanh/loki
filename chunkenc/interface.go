package chunkenc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql/log"
)

// Errors returned by the chunk interface.
var (
	ErrChunkFull       = errors.New("chunk full")
	ErrOutOfOrder      = errors.New("entry out of order")
	ErrInvalidSize     = errors.New("invalid size")
	ErrInvalidFlag     = errors.New("invalid flag")
	ErrInvalidChecksum = errors.New("invalid chunk checksum")
)

// Encoding is the identifier for a chunk encoding.
type Encoding byte

// The different available encodings.
// Make sure to preserve the order, as these numeric values are written to the chunks!
const (
	EncNone Encoding = iota
	EncGZIP
	EncDumb
	EncLZ4_64k
	EncSnappy
	EncLZ4_256k
	EncLZ4_1M
	EncLZ4_4M
	EncFlate
	EncZstd
)

var supportedEncoding = []Encoding{
	EncNone,
	EncGZIP,
	EncLZ4_64k,
	EncSnappy,
	EncLZ4_256k,
	EncLZ4_1M,
	EncLZ4_4M,
	EncFlate,
	EncZstd,
}

func (e Encoding) String() string {
	switch e {
	case EncGZIP:
		return "gzip"
	case EncNone:
		return "none"
	case EncDumb:
		return "dumb"
	case EncLZ4_64k:
		return "lz4-64k"
	case EncLZ4_256k:
		return "lz4-256k"
	case EncLZ4_1M:
		return "lz4-1M"
	case EncLZ4_4M:
		return "lz4"
	case EncSnappy:
		return "snappy"
	case EncFlate:
		return "flate"
	case EncZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

// ParseEncoding parses an chunk encoding (compression algorithm) by its name.
func ParseEncoding(enc string) (Encoding, error) {
	for _, e := range supportedEncoding {
		if strings.EqualFold(e.String(), enc) {
			return e, nil
		}
	}
	return 0, fmt.Errorf("invalid encoding: %s, supported: %s", enc, SupportedEncoding())

}

// SupportedEncoding returns the list of supported Encoding.
func SupportedEncoding() string {
	var sb strings.Builder
	for i := range supportedEncoding {
		sb.WriteString(supportedEncoding[i].String())
		if i != len(supportedEncoding)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

// Chunk is the interface for the compressed logs chunk format.
type Chunk interface {
	SpaceFor(*logproto.Entry) bool
	Append(*logproto.Entry) error
	Iterator(ctx context.Context, mintT, maxtT time.Time, direction logproto.Direction, pipeline log.StreamPipeline) (iter.EntryIterator, error)
	SampleIterator(ctx context.Context, from, through time.Time, extractor log.StreamSampleExtractor) iter.SampleIterator
	// Returns the list of blocks in the chunks.
	Blocks(mintT, maxtT time.Time) []Block
	io.WriterTo
	Utilization() float64
	UncompressedSize() int
	Close() error
	Encoding() Encoding
}

// Block is a chunk block.
type Block interface {
	// MinTime is the minimum time of entries in the block
	MinTime() int64
	// MaxTime is the maximum time of entries in the block
	MaxTime() int64
	// Offset is the offset/position of the block in the chunk. Offset is unique for a given block per chunk.
	Offset() int
	// Entries is the amount of entries in the block.
	Entries() int
	// Iterator returns an entry iterator for the block.
	Iterator(ctx context.Context, pipeline log.StreamPipeline) iter.EntryIterator
	// SampleIterator returns a sample iterator for the block.
	SampleIterator(ctx context.Context, extractor log.StreamSampleExtractor) iter.SampleIterator
}
