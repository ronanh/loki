package log

import (
	"strings"
)

func uniqueString(s []string) []string {
	l := len(s)
	for i := 0; i < l; i++ {
		for j := i + 1; j < l; j++ {
			// duplicate found: remove it
			if s[i] == s[j] {
				copy(s[j:], s[j+1:])
				l--
				break
			}
		}
	}
	return s[:l]
}

func sanitizeLabelKey(key string, isPrefix bool) string {
	if len(key) == 0 {
		return key
	}
	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return key
	}
	if isPrefix && key[0] >= '0' && key[0] <= '9' {
		key = "_" + key
	}
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, key)
}
