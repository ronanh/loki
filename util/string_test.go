package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStrToBytesRoundtrip(t *testing.T) {
	for _, s := range []string{
		"", "blabla", "###~u", "hello world", "$ƒ∂©ƒ∂ç√≈çå©",
	} {
		t.Run(s, func(t *testing.T) {
			b := StrToBytes(s)
			if b != nil {
				require.Equal(t, b, []byte(s))
			}
			after := BytesToStr(b)
			require.Equal(t, s, after)

			bb := StrToBytes(after)
			if b != nil {
				require.Equal(t, &b[0], &bb[0])
			}
		})
	}
}

func TestBytesToStrMutate(t *testing.T) {
	b := []byte("hello world")
	s := BytesToStr(b)
	b[0] = 'w'
	require.Equal(t, "wello world", s)
}
