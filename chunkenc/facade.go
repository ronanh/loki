package chunkenc

import (
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
)

// GzipLogChunk is a cortex encoding type for our chunks.
// Deprecated: the chunk encoding/compression format is inside the chunk data.
const GzipLogChunk = encoding.Encoding(128)

// LogChunk is a cortex encoding type for our chunks.
const LogChunk = encoding.Encoding(129)

func init() {
	encoding.MustRegisterEncoding(GzipLogChunk, "GzipLogChunk", func() encoding.Chunk {
		return &Facade{}
	})
	encoding.MustRegisterEncoding(LogChunk, "LogChunk", func() encoding.Chunk {
		return &Facade{}
	})
}

// Facade for compatibility with cortex chunk type, so we can use its chunk store.
type Facade struct {
	c          Chunk
	blockSize  int
	targetSize int
	encoding.Chunk
}

// NewFacade makes a new Facade.
func NewFacade(c Chunk, blockSize, targetSize int) encoding.Chunk {
	return &Facade{
		c:          c,
		blockSize:  blockSize,
		targetSize: targetSize,
	}
}

// LokiChunk returns the chunkenc.Chunk.
func (f Facade) LokiChunk() Chunk {
	return f.c
}
