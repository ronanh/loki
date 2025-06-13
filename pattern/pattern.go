package pattern

import (
	"bytes"
	"errors"
	"fmt"
	"unicode"
)

var (
	errSuccessiveCapturesNotAllowed = errors.New("cannot have 2 successive captures without at least 1 litteral in between")
	errZeroNamedCaptures            = errors.New("there must be at least 1 named capture")
	errZeroParts                    = errors.New("pattern contained no literals nor captures")
	errDuplicateCapture             = errors.New("found duplicate capture")
)

func isInvalidCaptureName(b []byte) bool {
	// empty is invalid
	if len(b) == 0 {
		return true
	}

	// this means that it is either an unnamed capture
	// or the capture's name starts with _. both are allowed
	if b[0] == '_' {
		return false
	}

	// the first char/rune cannot be a number
	if bytes.IndexFunc(b, func(r rune) bool {
		return unicode.IsDigit(r)
	}) == 0 {
		return true
	}

	// the rest of the chars must either be, letter, digit or underscore
	return bytes.ContainsFunc(b, func(r rune) bool {
		return !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_')
	})
}

type part struct {
	literal []byte
	capture []byte
}

func (p part) isUnnamedCapture() bool {
	return len(p.capture) == 1 && p.capture[0] == '_'
}

type Pattern struct {
	parts []part
}

func (pat *Pattern) Matches(input []byte) []MatchItem {
	output := make([]MatchItem, 0, len(pat.parts))
	iter := pat.Match(input)
	for {
		v, ok := iter.Next()
		if !ok {
			return output
		}
		if len(v.Key) == 1 && v.Key[0] == '_' {
			continue
		}
		output = append(output, v)
	}
}

func (pat *Pattern) Match(input []byte) MatchIter {
	return MatchIter{
		parts: pat.parts,
		input: input,
	}
}

type MatchIter struct {
	input []byte
	pos   int

	parts    []part
	currPart int
}

type MatchItem struct {
	Key   []byte
	Value []byte
}

// eatLiteralAnchor ensures that `input` matches
// the first literal of the provided pattern
//
// if the pattern starts with a capture, this doesn't get executed
func (iter *MatchIter) eatLiteralAnchor() bool {
	if iter.currPart != 0 || iter.parts[0].capture != nil {
		return true
	}

	literal := iter.parts[0].literal
	if len(literal) > len(iter.input) {
		return false
	}

	if bytes.Equal(literal, iter.input[:len(literal)]) {
		iter.currPart++
		iter.pos += len(literal)
		return true
	}
	return false
}

func (iter *MatchIter) Next() (MatchItem, bool) {
	// return immediately if the pattern starts
	// with a literal and the provided input
	// doesn't match that literal
	if ok := iter.eatLiteralAnchor(); !ok {
		return MatchItem{}, false
	}

	// the pattern is drained
	if len(iter.parts) == iter.currPart {
		return MatchItem{}, false
	}

	// since we ate the literal anchor
	// and there must be alternating
	// capture/literal, we are sure that
	// the current part is a capture
	captureKey := iter.parts[iter.currPart].capture

	iter.currPart++

	// the pattern ends with a capture -- take the remaining of the input
	if iter.currPart == len(iter.parts) {
		captureValue := iter.input[iter.pos:]
		iter.pos = len(iter.input)
		return MatchItem{Key: captureKey, Value: captureValue}, true
	}

	// we advanced to the next part of the pattern,
	// it must be literal because we just encountered
	// a capture
	literal := iter.parts[iter.currPart].literal
	pos := bytes.Index(iter.input[iter.pos:], literal)

	// if we didn't find the literal that delimits
	// this capture, we say that the capture contains the whole thing
	if pos == -1 {
		captureValue := iter.input[iter.pos:]
		iter.currPart = len(iter.parts)
		iter.pos = len(iter.input)
		return MatchItem{Key: captureKey, Value: captureValue}, true
	}

	captureValue := iter.input[iter.pos : iter.pos+pos]

	// skip to the next capture
	iter.pos += pos + len(literal)
	iter.currPart++

	return MatchItem{
		Key:   captureKey,
		Value: captureValue,
	}, true
}

func Compile(pat []byte) (*Pattern, error) {
	out, err := compileInternal(pat)
	if err != nil {
		return nil, fmt.Errorf("pattern \"%s\" was invalid: %w", pat, err)
	}
	return out, nil
}

func CompileFromString(pat string) (*Pattern, error) {
	return Compile([]byte(pat))
}

func compileInternal(pat []byte) (*Pattern, error) {
	namedCapturesCount := 0

	inCapture := false
	parts := make([]part, 0, 8)

	lhs := 0
	for i, v := range pat {
		switch v {
		case '<':
			if inCapture {
				lhs--
			}
			inCapture = true
			if lhs != 0 && lhs == i {
				return nil, errSuccessiveCapturesNotAllowed
			}
			if i != 0 {
				literal := pat[lhs:i]
				if len(parts) > 0 && parts[len(parts)-1].literal != nil {
					// We must ensure that we are always alternating between literal and capture
					// Here the previous part was already a literal
					// which means it needs to be extended with this one
					prevLiteral := parts[len(parts)-1].literal
					newLiteral := pat[lhs-len(prevLiteral) : i]
					parts[len(parts)-1].literal = newLiteral
				} else {
					parts = append(parts, part{literal: literal})
				}
			}
			lhs = i + 1
		case '>':
			if !inCapture {
				continue
			}
			if isInvalidCaptureName(pat[lhs:i]) {
				lhs--
				inCapture = false
				continue
			}
			capture := pat[lhs:i]
			inCapture = false
			part := part{capture: capture}
			if !part.isUnnamedCapture() {
				namedCapturesCount++
				for _, part := range parts {
					if bytes.Equal(part.capture, capture) {
						return nil, errDuplicateCapture
					}
				}
			}
			parts = append(parts, part)
			lhs = i + 1
		}
	}

	dangling := pat[lhs:]
	if len(dangling) != 0 {
		parts = append(parts, part{literal: dangling})
	}

	if len(parts) == 0 {
		return nil, errZeroParts
	}

	if namedCapturesCount == 0 {
		return nil, errZeroNamedCaptures
	}

	return &Pattern{parts: parts}, nil
}
