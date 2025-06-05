package util

import (
	"unsafe"
)

func UnsafeGetString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}
