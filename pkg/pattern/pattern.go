package pattern

import (
	"errors"
	"fmt"
)

var (
	errSuccessivePatternsNotAllowed  = errors.New("cannot have 2 successive patterns without 1 litteral in between")
	errUnexpectedOpenAngleBracket    = errors.New("encountered unexpected `<`")
	errUnexpectedClosingAngleBracket = errors.New("encountered unexpected `>`")
	errNoTokensDetected              = errors.New("pattern contained no literals nor captures")
	errIllegalCaracterInCapture      = errors.New("illegal caracter in capture name")
	errEmptyCaptureName              = errors.New("empty name for capture is not allowed. use `_` to discard a capture")
	errUnclosedCapture               = errors.New("reached end of pattern expression and capture was not closed")
	errIncompleteEscape              = errors.New("incomplete escape sequence at the end of pattern expression")
)

type part struct {
	literal []byte
	capture []byte
}

type Pattern struct {
	parts []part
}

func (pat *Pattern) Match(input []byte) {
	panic("not implemented")
}

func Compile(pat []byte) (*Pattern, error) {
	out, err := compileInternal(pat)
	if err != nil {
		return nil, fmt.Errorf("pattern \"%s\" was invalid: %w", pat, err)
	}
	return out, nil
}

func compileInternal(pat []byte) (*Pattern, error) {
	inCapture := false
	escape := false
	parts := make([]part, 0, 8)

	lhs := 0
	for i, v := range pat {
		if escape {
			escape = false
			continue
		}
		switch v {
		case '\\':
			if inCapture {
				return nil, errIllegalCaracterInCapture
			}
			escape = true
		case '<':
			if inCapture {
				return nil, errUnexpectedOpenAngleBracket
			}
			inCapture = true
			if lhs != 0 && lhs == i {
				return nil, errSuccessivePatternsNotAllowed
			}
			parts = append(parts, part{literal: pat[lhs:i]})
			lhs = i + 1
		case '>':
			if !inCapture {
				return nil, errUnexpectedClosingAngleBracket
			}
			capture := pat[lhs:i]
			if len(capture) == 0 {
				return nil, errEmptyCaptureName
			}
			inCapture = false
			parts = append(parts, part{capture: capture})
			lhs = i + 1
		}
	}

	if inCapture {
		return nil, errUnclosedCapture
	}

	if escape {
		return nil, errIncompleteEscape
	}

	dangling := pat[lhs:]
	if len(dangling) != 0 {
		parts = append(parts, part{literal: dangling})
	}

	if len(parts) == 0 {
		return nil, errNoTokensDetected
	}

	return &Pattern{parts: parts}, nil
}
