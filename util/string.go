package util

import (
	"unsafe"
)

// BytesToStr interprets `bs` as a utf8 encoded string
// without allocating a new buffer
//
// The returned string is mutated if the underlying buffer is mutated.
func BytesToStr(bs []byte) string {
	if len(bs) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(bs), len(bs))
}

// StrToBytes returns raw byte slice representation
// of `str` without allocation
//
// Mutating the returned buffer updates the `str`.
func StrToBytes(str string) []byte {
	if str == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(str), len(str))
}
