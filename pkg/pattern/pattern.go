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
)

type token struct {
	literal []byte
	capture []byte
}

type Pattern struct {
	tokens []token
}

func Compile(b []byte) (*Pattern, error) {
	out, err := compileInternal(b)
	if err != nil {
		return nil, fmt.Errorf("pattern \"%s\" was invalid: %w", b, err)
	}
	return out, nil
}

func compileInternal(b []byte) (*Pattern, error) {
	inCapture := false
	escape := false
	tokens := make([]token, 0, 8)

	i := 0
	for j, v := range b {
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
			if i != 0 && i == j {
				return nil, errSuccessivePatternsNotAllowed
			}
			tokens = append(tokens, token{literal: b[i:j]})
			i = j + 1
		case '>':
			if !inCapture {
				return nil, errUnexpectedClosingAngleBracket
			}
			capture := b[i:j]
			if len(capture) == 0 {
				return nil, errEmptyCaptureName
			}
			inCapture = false
			tokens = append(tokens, token{capture: capture})
			i = j + 1
		}
	}

	dangling := b[i:]
	if len(dangling) != 0 {
		tokens = append(tokens, token{literal: b[i:]})
	}

	if len(tokens) == 0 {
		return nil, errNoTokensDetected
	}

	return &Pattern{tokens}, nil
}
