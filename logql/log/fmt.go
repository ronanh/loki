package log

import (
	"bytes"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"text/template"
	"text/template/parse"

	"github.com/Masterminds/sprig/v3"
	"github.com/dustin/go-humanize"
	"github.com/ronanh/loki/util"
)

const lineFuncName = "__line__"

func init() {
	m := sprig.GenericFuncMap()
	for _, fn := range []string{
		"b64enc",
		"b64dec",
		"lower",
		"upper",
		"title",
		"trunc",
		"substr",
		"contains",
		"hasPrefix",
		"hasSuffix",
		"indent",
		"nindent",
		"replace",
		"repeat",
		"trim",
		"trimAll",
		"trimSuffix",
		"trimPrefix",
		"int",
		"float64",
		"add",
		"sub",
		"mul",
		"div",
		"mod",
		"addf",
		"subf",
		"mulf",
		"divf",
		"max",
		"min",
		"maxf",
		"minf",
		"ceil",
		"floor",
		"round",
		"fromJson",
		"date",
		"toDate",
		"now",
		"unixEpoch",
		"default",
		"regexReplaceAll",
		"regexReplaceAllLiteral",
	} {
		functionMap[fn] = m[fn]
	}
}

var (
	_ Stage = &LineFormatter{}
	_ Stage = &LabelsFormatter{}

	// Available map of functions for the text template engine.
	functionMap = template.FuncMap{
		// olds function deprecated.
		"ToLower":    strings.ToLower,
		"ToUpper":    strings.ToUpper,
		"Replace":    strings.Replace,
		"Trim":       strings.Trim,
		"TrimLeft":   strings.TrimLeft,
		"TrimRight":  strings.TrimRight,
		"TrimPrefix": strings.TrimPrefix,
		"TrimSuffix": strings.TrimSuffix,
		"TrimSpace":  strings.TrimSpace,

		// specific functions
		"urldecode": url.QueryUnescape,
		"urlencode": url.QueryEscape,
		"bytes": func(s string) (float64, error) {
			v, err := humanize.ParseBytes(s)
			return float64(v), err
		},
	}
)

type LineFormatter struct {
	currLine []byte

	*template.Template
	buf *bytes.Buffer
}

// NewFormatter creates a new log line formatter from a given text template.
func NewFormatter(tmpl string) (*LineFormatter, error) {
	lineFormatter := &LineFormatter{
		buf: bytes.NewBuffer(make([]byte, 4096)),
	}

	funcMap := maps.Clone(functionMap)
	funcMap[lineFuncName] = func() string {
		return util.BytesToStr(lineFormatter.currLine)
	}
	t, err := template.New("line").Option("missingkey=zero").Funcs(funcMap).Parse(tmpl)
	if err != nil {
		return nil, fmt.Errorf("invalid line template: %s", err)
	}
	lineFormatter.Template = t
	return lineFormatter, nil
}

func (lf *LineFormatter) Process(line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	lf.buf.Reset()
	m, tmpMap := lbs.Map()
	if tmpMap {
		defer smp.Put(m)
	}
	lf.currLine = line
	if err := lf.Template.Execute(lf.buf, m); err != nil {
		lbs.SetErr(errTemplateFormat)
		return line, true
	}
	// todo(cyriltovena): we might want to reuse the input line or a bytes buffer.
	res := make([]byte, len(lf.buf.Bytes()))
	copy(res, lf.buf.Bytes())
	return res, true
}

func (lf *LineFormatter) RequiredLabelNames() []string {
	return uniqueString(listNodeFields(lf.Root))
}

func listNodeFields(node parse.Node) []string {
	var res []string
	if node.Type() == parse.NodeAction {
		res = append(res, listNodeFieldsFromPipe(node.(*parse.ActionNode).Pipe)...)
	}
	res = append(res, listNodeFieldsFromBranch(node)...)
	if ln, ok := node.(*parse.ListNode); ok {
		for _, n := range ln.Nodes {
			res = append(res, listNodeFields(n)...)
		}
	}
	return res
}

func listNodeFieldsFromBranch(node parse.Node) []string {
	var res []string
	var b parse.BranchNode
	switch node.Type() {
	case parse.NodeIf:
		b = node.(*parse.IfNode).BranchNode
	case parse.NodeWith:
		b = node.(*parse.WithNode).BranchNode
	case parse.NodeRange:
		b = node.(*parse.RangeNode).BranchNode
	default:
		return res
	}
	if b.Pipe != nil {
		res = append(res, listNodeFieldsFromPipe(b.Pipe)...)
	}
	if b.List != nil {
		res = append(res, listNodeFields(b.List)...)
	}
	if b.ElseList != nil {
		res = append(res, listNodeFields(b.ElseList)...)
	}
	return res
}

func listNodeFieldsFromPipe(p *parse.PipeNode) []string {
	var res []string
	for _, c := range p.Cmds {
		for _, a := range c.Args {
			if f, ok := a.(*parse.FieldNode); ok {
				res = append(res, f.Ident...)
			}
		}
	}
	return res
}

// LabelFmt is a configuration struct for formatting a label.
type LabelFmt struct {
	Name  string
	Value string

	Rename bool
}

// NewRenameLabelFmt creates a configuration to rename a label.
func NewRenameLabelFmt(dst, target string) LabelFmt {
	return LabelFmt{
		Name:   dst,
		Rename: true,
		Value:  target,
	}
}

// NewTemplateLabelFmt creates a configuration to format a label using text template.
func NewTemplateLabelFmt(dst, template string) LabelFmt {
	return LabelFmt{
		Name:   dst,
		Rename: false,
		Value:  template,
	}
}

type labelFormatter struct {
	tmpl *template.Template
	LabelFmt
}

type LabelsFormatter struct {
	currentLine []byte

	formats []labelFormatter
	buf     *bytes.Buffer
}

// NewLabelsFormatter creates a new formatter that can format multiple labels at once.
// Either by renaming or using text template.
// It is not allowed to reformat the same label twice within the same formatter.
func NewLabelsFormatter(fmts []LabelFmt) (*LabelsFormatter, error) {
	if err := validate(fmts); err != nil {
		return nil, err
	}
	lblsFormatter := &LabelsFormatter{
		buf: bytes.NewBuffer(make([]byte, 1024)),
	}
	funcMap := maps.Clone(functionMap)
	funcMap[lineFuncName] = func() string {
		return util.BytesToStr(lblsFormatter.currentLine)
	}

	formats := make([]labelFormatter, 0, len(fmts))
	for _, fm := range fmts {
		toAdd := labelFormatter{LabelFmt: fm}
		if !fm.Rename {
			t, err := template.New("label").Option("missingkey=zero").Funcs(funcMap).Parse(fm.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid template for label '%s': %s", fm.Name, err)
			}
			toAdd.tmpl = t
		}
		formats = append(formats, toAdd)
	}
	lblsFormatter.formats = formats
	return lblsFormatter, nil
}

func validate(fmts []LabelFmt) error {
	// it would be too confusing to rename and change the same label value.
	// To avoid confusion we allow to have a label name only once per stage.
	uniqueLabelName := map[string]struct{}{}
	for _, f := range fmts {
		if f.Name == ErrorLabel {
			return fmt.Errorf("%s cannot be formatted", f.Name)
		}
		if _, ok := uniqueLabelName[f.Name]; ok {
			return fmt.Errorf(
				"multiple label name '%s' not allowed in a single format operation",
				f.Name,
			)
		}
		uniqueLabelName[f.Name] = struct{}{}
	}
	return nil
}

func (lf *LabelsFormatter) Process(l []byte, lbs *LabelsBuilder) ([]byte, bool) {
	lf.currentLine = l
	var tmpMap bool
	var m map[string]string
	for _, f := range lf.formats {
		if f.Rename {
			v, ok := lbs.Get(f.Value)
			if ok {
				lbs.Set(f.Name, v)
				lbs.Del(f.Value)
			}
			continue
		}
		lf.buf.Reset()
		if m == nil {
			// map is retrieved from pool
			m, tmpMap = lbs.Map()
		}
		if err := f.tmpl.Execute(lf.buf, m); err != nil {
			lbs.SetErr(errTemplateFormat)
			continue
		}
		lbs.Set(f.Name, lf.buf.String())
	}
	if m != nil && tmpMap {
		// return map to pool
		smp.Put(m)
	}
	return l, true
}

func (lf *LabelsFormatter) RequiredLabelNames() []string {
	var names []string
	for _, fm := range lf.formats {
		if fm.Rename {
			names = append(names, fm.Value)
			continue
		}
		names = append(names, listNodeFields(fm.tmpl.Root)...)
	}
	return uniqueString(names)
}
