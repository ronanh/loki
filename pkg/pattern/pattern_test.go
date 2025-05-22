package pattern

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompile(t *testing.T) {

	for _, scenario := range []struct {
		in  string
		out *Pattern
		err error
	}{
		{
			in:  "babab<whatsupp",
			out: nil,
			err: errUnclosedCapture,
		},
		{
			in:  "ciao<hola>mamamia\\",
			out: nil,
			err: errIncompleteEscape,
		},
		{
			in:  "abc<>",
			out: nil,
			err: errEmptyCaptureName,
		},
		{
			in: "abc<example>xddd",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("abc"),
					},
					{
						capture: []byte("example"),
					},
					{
						literal: []byte("xddd"),
					},
				},
			},
		},
		{
			in: "abc<def>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("abc"),
					},
					{
						capture: []byte("def"),
					},
				},
			},
		},
		{
			in:  "sss>",
			out: nil,
			err: errUnexpectedClosingAngleBracket,
		},
		{
			in:  ">",
			out: nil,
			err: errUnexpectedClosingAngleBracket,
		},
		{
			in:  "abc<ss<xd>",
			out: nil,
			err: errUnexpectedOpenAngleBracket,
		},
		{
			in: "ab\\<c<ssxd>blabla\\>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("ab\\<c"),
					},
					{
						capture: []byte("ssxd"),
					},
					{
						literal: []byte("blabla\\>"),
					},
				},
			},
			err: nil,
		},
		{
			in:  "ab<capture1><capture2>",
			out: nil,
			err: errSuccessivePatternsNotAllowed,
		},
		{
			in: "ab<capture1> <capture2>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("ab"),
					},
					{
						capture: []byte("capture1"),
					},
					{
						literal: []byte(" "),
					},
					{
						capture: []byte("capture2"),
					},
				},
			},
			err: nil,
		},
	} {
		t.Run(scenario.in, func(t *testing.T) {
			v, e := Compile([]byte(scenario.in))
			assert.ErrorIs(t, e, scenario.err)
			assert.Equal(t, scenario.out, v)
		})
	}

}
