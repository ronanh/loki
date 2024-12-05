package logql

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/ronanh/loki/pkg/iter"
	"github.com/ronanh/loki/pkg/logproto"
	"github.com/ronanh/loki/pkg/logql/log"
)

// Expr is the root expression which can be a SampleExpr or LogSelectorExpr
type Expr interface {
	logQLExpr()      // ensure it's not implemented accidentally
	Shardable() bool // A recursive check on the AST to see if it's shardable.
	fmt.Stringer
}

type ExprNode interface {
	Leaves() []Expr
}

type QueryParams interface {
	LogSelector() (LogSelectorExpr, error)
	GetStart() time.Time
	GetEnd() time.Time
	GetShards() []string
}

// implicit holds default implementations
type implicit struct{}

func (implicit) logQLExpr() {}

// SelectParams specifies parameters passed to data selections.
type SelectLogParams struct {
	*logproto.QueryRequest
}

// LogSelector returns the LogSelectorExpr from the SelectParams.
// The `LogSelectorExpr` can then returns all matchers and filters to use for that request.
func (s SelectLogParams) LogSelector() (LogSelectorExpr, error) {
	return ParseLogSelector(s.Selector)
}

type SelectSampleParams struct {
	*logproto.SampleQueryRequest
}

// Expr returns the SampleExpr from the SelectSampleParams.
// The `LogSelectorExpr` can then returns all matchers and filters to use for that request.
func (s SelectSampleParams) Expr() (SampleExpr, error) {
	return ParseSampleExpr(s.Selector)
}

// LogSelector returns the LogSelectorExpr from the SelectParams.
// The `LogSelectorExpr` can then returns all matchers and filters to use for that request.
func (s SelectSampleParams) LogSelector() (LogSelectorExpr, error) {
	expr, err := ParseSampleExpr(s.Selector)
	if err != nil {
		return nil, err
	}
	return expr.Selector(), nil
}

// Querier allows a LogQL expression to fetch an EntryIterator for a
// set of matchers and filters
type Querier interface {
	SelectLogs(context.Context, SelectLogParams) (iter.EntryIterator, error)
	SelectSamples(context.Context, SelectSampleParams) (iter.SampleIterator, error)
}

// LogSelectorExpr is a LogQL expression filtering and returning logs.
type LogSelectorExpr interface {
	Matchers() []*labels.Matcher
	PipelineExpr
	HasFilter() bool
	Expr
}

// Type alias for backward compatibility
type (
	Pipeline        = log.Pipeline
	SampleExtractor = log.SampleExtractor
)

// PipelineExpr is an expression defining a log pipeline.
type PipelineExpr interface {
	Pipeline() (Pipeline, error)
	Expr
}

// StageExpr is an expression defining a single step into a log pipeline
type StageExpr interface {
	Stage() (log.Stage, error)
	Expr
}

// MultiStageExpr is multiple stages which implement a PipelineExpr.
type MultiStageExpr []StageExpr

func (m MultiStageExpr) Pipeline() (log.Pipeline, error) {
	stages, err := m.stages()
	if err != nil {
		return nil, err
	}
	return log.NewPipeline(stages), nil
}

func (m MultiStageExpr) stages() ([]log.Stage, error) {
	c := make([]log.Stage, 0, len(m))
	for _, e := range m {
		p, err := e.Stage()
		if err != nil {
			return nil, newStageError(e, err)
		}
		if p == log.NoopStage {
			continue
		}
		c = append(c, p)
	}
	return c, nil
}

func (m MultiStageExpr) String() string {
	var sb strings.Builder
	for i, e := range m {
		sb.WriteString(e.String())
		if i+1 != len(m) {
			sb.WriteString(" ")
		}
	}
	return sb.String()
}

func (MultiStageExpr) logQLExpr() {} // nolint:unused

type matchersExpr struct {
	matchers []*labels.Matcher
	implicit
}

func newMatcherExpr(matchers []*labels.Matcher) *matchersExpr {
	return &matchersExpr{matchers: matchers}
}

func (e *matchersExpr) Matchers() []*labels.Matcher {
	return e.matchers
}

func (e *matchersExpr) AddMatcher(matcher *labels.Matcher) {
	e.matchers = append(e.matchers, matcher)
}

func (e *matchersExpr) Shardable() bool { return true }

func (e *matchersExpr) String() string {
	var sb strings.Builder
	sb.Grow(len(e.matchers) * 32)
	sb.WriteString("{")
	for i, m := range e.matchers {
		// labels.Matcher.String() is slow, so we avoid it here.
		// sb.WriteString(m.String())
		sb.WriteString(m.Name)
		switch m.Type {
		case labels.MatchEqual:
			sb.WriteString("=")
		case labels.MatchNotEqual:
			sb.WriteString("!=")
		case labels.MatchRegexp:
			sb.WriteString("=~")
		case labels.MatchNotRegexp:
			sb.WriteString("!~")
		}
		StringBuilderQuoteTo(&sb, m.Value)
		// sb.WriteString(strconv.Quote(m.Value))
		if i+1 != len(e.matchers) {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("}")
	return sb.String()
}

func (e *matchersExpr) Pipeline() (log.Pipeline, error) {
	return log.NewNoopPipeline(), nil
}

func (e *matchersExpr) HasFilter() bool {
	return false
}

type PipelineBuilder interface {
	AddLabelFilterer(labelFilterer log.LabelFilterer)
}

type MatchersBuilder interface {
	Matchers() []*labels.Matcher
	AddMatcher(matcher *labels.Matcher)
}

type pipelineExpr struct {
	pipeline MultiStageExpr
	left     *matchersExpr
	implicit
}

func newPipelineExpr(left *matchersExpr, pipeline MultiStageExpr) LogSelectorExpr {
	return &pipelineExpr{
		left:     left,
		pipeline: pipeline,
	}
}

func (e *pipelineExpr) Shardable() bool {
	for _, p := range e.pipeline {
		if !p.Shardable() {
			return false
		}
	}
	return true
}

func (e *pipelineExpr) Matchers() []*labels.Matcher {
	return e.left.Matchers()
}

func (e *pipelineExpr) Leaves() []Expr {
	return []Expr{e.left}
}

// func (e *pipelineExpr) AddMatcher(matcher *labels.Matcher) {
// 	e.left.matchers = append(e.left.matchers, matcher)
// }

func (e *pipelineExpr) AddLabelFilterer(labelFilterer log.LabelFilterer) {
	e.pipeline = append(e.pipeline, &labelFilterExpr{LabelFilterer: labelFilterer})
}

func (e *pipelineExpr) String() string {
	var sb strings.Builder
	sb.WriteString(e.left.String())
	sb.WriteString(" ")
	sb.WriteString(e.pipeline.String())
	return sb.String()
}

func (e *pipelineExpr) Pipeline() (log.Pipeline, error) {
	return e.pipeline.Pipeline()
}

// HasFilter returns true if the pipeline contains stage that can filter out lines.
func (e *pipelineExpr) HasFilter() bool {
	for _, p := range e.pipeline {
		switch p.(type) {
		case *lineFilterExpr, *labelFilterExpr:
			return true
		default:
			continue
		}
	}
	return false
}

type lineFilterExpr struct {
	left  *lineFilterExpr
	ty    labels.MatchType
	match string
	implicit
}

func newLineFilterExpr(left *lineFilterExpr, ty labels.MatchType, match string) *lineFilterExpr {
	return &lineFilterExpr{
		left:  left,
		ty:    ty,
		match: match,
	}
}

// AddFilterExpr adds a filter expression to a logselector expression.
func AddFilterExpr(expr LogSelectorExpr, ty labels.MatchType, match string) (LogSelectorExpr, error) {
	filter := newLineFilterExpr(nil, ty, match)
	switch e := expr.(type) {
	case *matchersExpr:
		return newPipelineExpr(e, MultiStageExpr{filter}), nil
	case *pipelineExpr:
		e.pipeline = append(e.pipeline, filter)
		return e, nil
	default:
		return nil, fmt.Errorf("unknown LogSelector: %v+", expr)
	}
}

func (e *lineFilterExpr) Leaves() []Expr {
	if e.left == nil {
		return []Expr{e}
	}
	return e.left.Leaves()
}

func (e *lineFilterExpr) Shardable() bool { return true }

func (e *lineFilterExpr) String() string {
	var sb strings.Builder
	sb.Grow(32)
	if e.left != nil {
		sb.WriteString(e.left.String())
		sb.WriteString(" ")
	}
	switch e.ty {
	case labels.MatchRegexp:
		sb.WriteString("|~")
	case labels.MatchNotRegexp:
		sb.WriteString("!~")
	case labels.MatchEqual:
		sb.WriteString("|=")
	case labels.MatchNotEqual:
		sb.WriteString("!=")
	}
	sb.WriteString(" ")
	StringBuilderQuoteTo(&sb, e.match)
	// sb.WriteString(strconv.Quote(e.match))
	return sb.String()
}

func (e *lineFilterExpr) Filter() (log.Filterer, error) {
	f, err := log.NewFilter(e.match, e.ty)
	if err != nil {
		return nil, err
	}
	if e.left != nil {
		nextFilter, err := e.left.Filter()
		if err != nil {
			return nil, err
		}
		if nextFilter != nil {
			f = log.NewAndFilter(nextFilter, f)
		}
	}

	return f, nil
}

func (e *lineFilterExpr) Stage() (log.Stage, error) {
	f, err := e.Filter()
	if err != nil {
		return nil, err
	}
	return f.ToStage(), nil
}

type labelParserExpr struct {
	op    string
	param string
	implicit
}

func newLabelParserExpr(op, param string) *labelParserExpr {
	return &labelParserExpr{
		op:    op,
		param: param,
	}
}

func (e *labelParserExpr) Shardable() bool { return true }

func (e *labelParserExpr) Stage() (log.Stage, error) {
	switch e.op {
	case OpParserTypeJSON:
		return log.NewJSONParser(), nil
	case OpParserTypeLogfmt:
		return log.NewLogfmtParser(), nil
	case OpParserTypeRegexp:
		return log.NewRegexpParser(e.param)
	case OpParserTypeUnpack:
		return log.NewUnpackParser(), nil
	default:
		return nil, fmt.Errorf("unknown parser operator: %s", e.op)
	}
}

func (e *labelParserExpr) String() string {
	var sb strings.Builder
	sb.Grow(32)
	sb.WriteString(OpPipe)
	sb.WriteString(" ")
	sb.WriteString(e.op)
	if e.param != "" {
		sb.WriteString(" ")
		StringBuilderQuoteTo(&sb, e.param)
		// sb.WriteString(strconv.Quote(e.param))
	}
	return sb.String()
}

type labelFilterExpr struct {
	log.LabelFilterer
	implicit
}

func (e *labelFilterExpr) Shardable() bool { return true }

func (e *labelFilterExpr) Stage() (log.Stage, error) {
	return e.LabelFilterer, nil
}

func (e *labelFilterExpr) String() string {
	return fmt.Sprintf("%s %s", OpPipe, e.LabelFilterer.String())
}

type DropLabelsExpr struct {
	dropLabels []log.DropLabel
	implicit
}

func newDropLabelsExpr(dropLabels []log.DropLabel) *DropLabelsExpr {
	return &DropLabelsExpr{dropLabels: dropLabels}
}

func (e *DropLabelsExpr) Shardable() bool { return true }

func (e *DropLabelsExpr) Stage() (log.Stage, error) {
	return log.NewDropLabels(e.dropLabels), nil
}

func (e *DropLabelsExpr) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s %s ", OpPipe, OpDrop))

	for i, dropLabel := range e.dropLabels {
		if i > 0 {
			sb.WriteString(",")
		}
		if dropLabel.Matcher != nil {
			sb.WriteString(dropLabel.Matcher.String())
		}
		if dropLabel.Name != "" {
			sb.WriteString(dropLabel.Name)
		}
	}
	str := sb.String()
	return str
}

type lineFmtExpr struct {
	value string
	implicit
}

func newLineFmtExpr(value string) *lineFmtExpr {
	return &lineFmtExpr{
		value: value,
	}
}

func (e *lineFmtExpr) Shardable() bool { return true }

func (e *lineFmtExpr) Stage() (log.Stage, error) {
	return log.NewFormatter(e.value)
}

func (e *lineFmtExpr) String() string {
	return OpPipe + " " + OpFmtLine + " " + strconv.Quote(e.value)
}

type labelFmtExpr struct {
	formats []log.LabelFmt

	implicit
}

func newLabelFmtExpr(fmts []log.LabelFmt) *labelFmtExpr {
	return &labelFmtExpr{
		formats: fmts,
	}
}

func (e *labelFmtExpr) Shardable() bool { return false }

func (e *labelFmtExpr) Stage() (log.Stage, error) {
	return log.NewLabelsFormatter(e.formats)
}

func (e *labelFmtExpr) String() string {
	var sb strings.Builder
	sb.Grow(32)
	sb.WriteString(OpPipe)
	sb.WriteString(" ")
	sb.WriteString(OpFmtLabel)
	sb.WriteString(" ")
	// sb.WriteString(fmt.Sprintf("%s %s ", OpPipe, OpFmtLabel))
	for i, f := range e.formats {
		sb.WriteString(f.Name)
		sb.WriteString("=")
		if f.Rename {
			sb.WriteString(f.Value)
		} else {
			StringBuilderQuoteTo(&sb, f.Value)
			// sb.WriteString(strconv.Quote(f.Value))
		}
		if i+1 != len(e.formats) {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

type jsonExpressionParser struct {
	expressions []log.JSONExpression

	implicit
}

func newJSONExpressionParser(expressions []log.JSONExpression) *jsonExpressionParser {
	return &jsonExpressionParser{
		expressions: expressions,
	}
}

func (j *jsonExpressionParser) Shardable() bool { return true }

func (j *jsonExpressionParser) Stage() (log.Stage, error) {
	return log.NewJSONExpressionParser(j.expressions)
}

func (j *jsonExpressionParser) String() string {
	var sb strings.Builder
	sb.Grow(32)
	sb.WriteString(OpPipe)
	sb.WriteString(" ")
	sb.WriteString(OpParserTypeJSON)
	sb.WriteString(" ")
	// sb.WriteString(fmt.Sprintf("%s %s ", OpPipe, OpParserTypeJSON))
	for i, exp := range j.expressions {
		sb.WriteString(exp.Identifier)
		sb.WriteString("=")
		StringBuilderQuoteTo(&sb, exp.Expression)
		// sb.WriteString(strconv.Quote(exp.Expression))

		if i+1 != len(j.expressions) {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func mustNewMatcher(t labels.MatchType, n, v string) *labels.Matcher {
	m, err := labels.NewMatcher(t, n, v)
	if err != nil {
		panic(newParseError(err.Error(), 0, 0))
	}
	return m
}

func mustNewFloat(s string) float64 {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(newParseError(fmt.Sprintf("unable to parse float: %s", err.Error()), 0, 0))
	}
	return n
}

type unwrapExpr struct {
	identifier string
	operation  string

	postFilters []log.LabelFilterer
}

func (u unwrapExpr) String() string {
	var sb strings.Builder
	if u.operation != "" {
		sb.WriteString(fmt.Sprintf(" %s %s %s(%s)", OpPipe, OpUnwrap, u.operation, u.identifier))
	} else {
		sb.WriteString(fmt.Sprintf(" %s %s %s", OpPipe, OpUnwrap, u.identifier))
	}
	for _, f := range u.postFilters {
		sb.WriteString(fmt.Sprintf(" %s %s", OpPipe, f))
	}
	return sb.String()
}

func (u *unwrapExpr) addPostFilter(f log.LabelFilterer) *unwrapExpr {
	u.postFilters = append(u.postFilters, f)
	return u
}

func newUnwrapExpr(id string, operation string) *unwrapExpr {
	return &unwrapExpr{identifier: id, operation: operation}
}

type LogRange interface {
	Interval() time.Duration
}

type logRange struct {
	left     LogSelectorExpr
	interval time.Duration

	unwrap *unwrapExpr
}

// impls Stringer
func (r logRange) String() string {
	left := r.left.String()
	interval := model.Duration(r.interval).String()
	if r.unwrap != nil {
		unwrap := r.unwrap.String()
		return left + unwrap + "[" + interval + "]"
	}
	return left + "[" + interval + "]"
}

func (r *logRange) Shardable() bool { return r.left.Shardable() }

func newLogRange(left LogSelectorExpr, interval time.Duration, u *unwrapExpr) *logRange {
	return &logRange{
		left:     left,
		interval: interval,
		unwrap:   u,
	}
}

const (
	// vector ops
	OpTypeSum     = "sum"
	OpTypeAvg     = "avg"
	OpTypeMax     = "max"
	OpTypeMin     = "min"
	OpTypeCount   = "count"
	OpTypeStddev  = "stddev"
	OpTypeStdvar  = "stdvar"
	OpTypeBottomK = "bottomk"
	OpTypeTopK    = "topk"

	// range vector ops
	OpRangeTypeCount     = "count_over_time"
	OpRangeTypeRate      = "rate"
	OpRangeTypeBytes     = "bytes_over_time"
	OpRangeTypeBytesRate = "bytes_rate"
	OpRangeTypeAvg       = "avg_over_time"
	OpRangeTypeSum       = "sum_over_time"
	OpRangeTypeMin       = "min_over_time"
	OpRangeTypeMax       = "max_over_time"
	OpRangeTypeStdvar    = "stdvar_over_time"
	OpRangeTypeStddev    = "stddev_over_time"
	OpRangeTypeQuantile  = "quantile_over_time"
	OpRangeTypeAbsent    = "absent_over_time"

	// binops - logical/set
	OpTypeOr     = "or"
	OpTypeAnd    = "and"
	OpTypeUnless = "unless"

	// binops - operations
	OpTypeAdd = "+"
	OpTypeSub = "-"
	OpTypeMul = "*"
	OpTypeDiv = "/"
	OpTypeMod = "%"
	OpTypePow = "^"

	// binops - comparison
	OpTypeCmpEQ = "=="
	OpTypeNEQ   = "!="
	OpTypeGT    = ">"
	OpTypeGTE   = ">="
	OpTypeLT    = "<"
	OpTypeLTE   = "<="

	// parsers
	OpParserTypeJSON   = "json"
	OpParserTypeLogfmt = "logfmt"
	OpParserTypeRegexp = "regexp"
	OpParserTypeUnpack = "unpack"

	OpFmtLine  = "line_format"
	OpFmtLabel = "label_format"

	OpPipe   = "|"
	OpUnwrap = "unwrap"

	// conversion Op
	OpConvBytes           = "bytes"
	OpConvDuration        = "duration"
	OpConvDurationSeconds = "duration_seconds"

	OpLabelReplace = "label_replace"

	// drop
	OpDrop = "drop"
)

func IsComparisonOperator(op string) bool {
	switch op {
	case OpTypeCmpEQ, OpTypeNEQ, OpTypeGT, OpTypeGTE, OpTypeLT, OpTypeLTE:
		return true
	default:
		return false
	}
}

// IsLogicalBinOp tests whether an operation is a logical/set binary operation
func IsLogicalBinOp(op string) bool {
	switch op {
	case OpTypeOr, OpTypeAnd, OpTypeUnless:
		return true
	default:
		return false
	}
}

// SampleExpr is a LogQL expression filtering logs and returning metric samples.
type SampleExpr interface {
	// Selector is the LogQL selector to apply when retrieving logs.
	Selector() LogSelectorExpr
	Extractor() (SampleExtractor, error)
	Expr
}

type rangeAggregationExpr struct {
	left      *logRange
	operation string

	params   *float64
	grouping *grouping
	implicit
}

var _ GroupBuilder = &rangeAggregationExpr{}

func newRangeAggregationExpr(left *logRange, operation string, gr *grouping, stringParams *string) SampleExpr {
	var params *float64
	if stringParams != nil {
		if operation != OpRangeTypeQuantile {
			panic(newParseError(fmt.Sprintf("parameter %s not supported for operation %s", *stringParams, operation), 0, 0))
		}
		var err error
		params = new(float64)
		*params, err = strconv.ParseFloat(*stringParams, 64)
		if err != nil {
			panic(newParseError(fmt.Sprintf("invalid parameter for operation %s: %s", operation, err), 0, 0))
		}

	} else {
		if operation == OpRangeTypeQuantile {
			panic(newParseError(fmt.Sprintf("parameter required for operation %s", operation), 0, 0))
		}
	}
	e := &rangeAggregationExpr{
		left:      left,
		operation: operation,
		grouping:  gr,
		params:    params,
	}
	if err := e.validate(); err != nil {
		panic(newParseError(err.Error(), 0, 0))
	}
	return e
}

func (e *rangeAggregationExpr) Selector() LogSelectorExpr {
	return e.left.left
}

func (e *rangeAggregationExpr) Leaves() []Expr {
	return []Expr{e.left.left}
}

func (e *rangeAggregationExpr) Interval() time.Duration {
	return e.left.interval
}

func (e rangeAggregationExpr) validate() error {
	if e.grouping != nil {
		switch e.operation {
		case OpRangeTypeAvg, OpRangeTypeStddev, OpRangeTypeStdvar, OpRangeTypeQuantile, OpRangeTypeMax, OpRangeTypeMin:
		default:
			return fmt.Errorf("grouping not allowed for %s aggregation", e.operation)
		}
	}
	if e.left.unwrap != nil {
		switch e.operation {
		case OpRangeTypeRate, OpRangeTypeAvg, OpRangeTypeSum, OpRangeTypeMax, OpRangeTypeMin, OpRangeTypeStddev, OpRangeTypeStdvar, OpRangeTypeQuantile, OpRangeTypeAbsent:
			return nil
		default:
			return fmt.Errorf("invalid aggregation %s with unwrap", e.operation)
		}
	}
	switch e.operation {
	case OpRangeTypeBytes, OpRangeTypeBytesRate, OpRangeTypeCount, OpRangeTypeRate, OpRangeTypeAbsent:
		return nil
	default:
		return fmt.Errorf("invalid aggregation %s without unwrap", e.operation)
	}
}

func (e *rangeAggregationExpr) HasGrouping() bool {
	return e.grouping != nil
}

func (e *rangeAggregationExpr) Groups() []string {
	return e.grouping.groups
}

func (e *rangeAggregationExpr) Without() bool {
	return e.grouping.without
}

func (e *rangeAggregationExpr) AddGroup(group string) {
	e.grouping.groups = append(e.grouping.groups, group)
}

// impls Stringer
func (e *rangeAggregationExpr) String() string {
	left := e.left.String()
	if e.params != nil && e.grouping != nil {
		grouping := e.grouping.String()
		params := strconv.FormatFloat(*e.params, 'f', -1, 64)
		return e.operation + "(" + params + "," + left + ")" + grouping
	} else if e.params != nil {
		params := strconv.FormatFloat(*e.params, 'f', -1, 64)
		return e.operation + "(" + params + "," + left + ")"
	} else if e.grouping != nil {
		grouping := e.grouping.String()
		return e.operation + "(" + left + ")" + grouping
	}
	return e.operation + "(" + left + ")"
}

// impl SampleExpr
func (e *rangeAggregationExpr) Shardable() bool {
	return shardableOps[e.operation] && e.left.Shardable()
}

type GroupBuilder interface {
	HasGrouping() bool
	Groups() []string
	Without() bool
	AddGroup(string)
}

type grouping struct {
	groups  []string
	without bool
}

// impls Stringer
func (g grouping) String() string {
	if len(g.groups) == 0 && !g.without {
		return ""
	}
	var sb strings.Builder
	sb.Grow(16 + len(g.groups)*16)
	if g.without {
		sb.WriteString(" without")
	} else if len(g.groups) > 0 {
		sb.WriteString(" by")
	}

	if len(g.groups) > 0 {
		sb.WriteString("(")
		for i, group := range g.groups {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(group)
		}
		// sb.WriteString(strings.Join(g.groups, ","))
		sb.WriteString(")")
	}

	return sb.String()
}

type vectorAggregationExpr struct {
	left SampleExpr

	grouping  *grouping
	params    int
	operation string
	implicit
}

var _ GroupBuilder = &vectorAggregationExpr{}

func mustNewVectorAggregationExpr(left SampleExpr, operation string, gr *grouping, params *string) SampleExpr {
	var p int
	var err error
	switch operation {
	case OpTypeBottomK, OpTypeTopK:
		if params == nil {
			panic(newParseError(fmt.Sprintf("parameter required for operation %s", operation), 0, 0))
		}
		if p, err = strconv.Atoi(*params); err != nil {
			panic(newParseError(fmt.Sprintf("invalid parameter %s(%s,", operation, *params), 0, 0))
		}

	default:
		if params != nil {
			panic(newParseError(fmt.Sprintf("unsupported parameter for operation %s(%s,", operation, *params), 0, 0))
		}
	}
	if gr == nil {
		gr = &grouping{}
	}
	return &vectorAggregationExpr{
		left:      left,
		operation: operation,
		grouping:  gr,
		params:    p,
	}
}

func (e *vectorAggregationExpr) Selector() LogSelectorExpr {
	return e.left.Selector()
}

func (e *vectorAggregationExpr) Extractor() (log.SampleExtractor, error) {
	// inject in the range vector extractor the outer groups to improve performance.
	// This is only possible if the operation is a sum. Anything else needs all labels.
	if r, ok := e.left.(*rangeAggregationExpr); ok && canInjectVectorGrouping(e.operation, r.operation) {
		// if the range vec operation has no grouping we can push down the vec one.
		if r.grouping == nil {
			return r.extractor(e.grouping)
		}
	}
	return e.left.Extractor()
}

func (e *vectorAggregationExpr) Leaves() []Expr {
	return []Expr{e.left}
}

// canInjectVectorGrouping tells if a vector operation can inject grouping into the nested range vector.
func canInjectVectorGrouping(vecOp, rangeOp string) bool {
	if vecOp != OpTypeSum {
		return false
	}
	switch rangeOp {
	case OpRangeTypeBytes, OpRangeTypeBytesRate, OpRangeTypeSum, OpRangeTypeRate, OpRangeTypeCount:
		return true
	default:
		return false
	}
}

func (e *vectorAggregationExpr) HasGrouping() bool {
	return e.grouping != nil
}

func (e *vectorAggregationExpr) Groups() []string {
	return e.grouping.groups
}

func (e *vectorAggregationExpr) Without() bool {
	return e.grouping.without
}

func (e *vectorAggregationExpr) AddGroup(group string) {
	e.grouping.groups = append(e.grouping.groups, group)
}

func (e *vectorAggregationExpr) String() string {
	if e.params != 0 {
		if e.grouping != nil {
			return e.operation + e.grouping.String() + "(" + strconv.Itoa(e.params) + "," + e.left.String() + ")"
		}
		return e.operation + "(" + strconv.Itoa(e.params) + "," + e.left.String() + ")"
	}
	if e.grouping != nil {
		return e.operation + e.grouping.String() + "(" + e.left.String() + ")"
	}
	return e.operation + "(" + e.left.String() + ")"
}

// impl SampleExpr
func (e *vectorAggregationExpr) Shardable() bool {
	return shardableOps[e.operation] && e.left.Shardable()
}

type BinOpOptions struct {
	ReturnBool bool
}

type binOpExpr struct {
	SampleExpr
	RHS  SampleExpr
	op   string
	opts BinOpOptions
}

func (e *binOpExpr) String() string {
	expr := e.SampleExpr.String()
	rhs := e.RHS.String()
	if e.opts.ReturnBool {
		return "(" + expr + " " + e.op + " bool " + rhs + ")"
	}
	return "(" + expr + " " + e.op + " " + rhs + ")"
}

func (e *binOpExpr) Leaves() []Expr {
	return []Expr{e.SampleExpr, e.RHS}
}

// impl SampleExpr
func (e *binOpExpr) Shardable() bool {
	return shardableOps[e.op] && e.SampleExpr.Shardable() && e.RHS.Shardable()
}

func mustNewBinOpExpr(op string, opts BinOpOptions, lhs, rhs Expr) SampleExpr {
	left, ok := lhs.(SampleExpr)
	if !ok {
		panic(newParseError(fmt.Sprintf(
			"unexpected type for left leg of binary operation (%s): %T",
			op,
			lhs,
		), 0, 0))
	}

	right, ok := rhs.(SampleExpr)
	if !ok {
		panic(newParseError(fmt.Sprintf(
			"unexpected type for right leg of binary operation (%s): %T",
			op,
			rhs,
		), 0, 0))
	}

	leftLit, lOk := left.(*literalExpr)
	rightLit, rOk := right.(*literalExpr)

	if IsLogicalBinOp(op) {
		if lOk {
			panic(newParseError(fmt.Sprintf(
				"unexpected literal for left leg of logical/set binary operation (%s): %f",
				op,
				leftLit.value,
			), 0, 0))
		}

		if rOk {
			panic(newParseError(fmt.Sprintf(
				"unexpected literal for right leg of logical/set binary operation (%s): %f",
				op,
				rightLit.value,
			), 0, 0))
		}
	}

	// map expr like (1+1) -> 2
	if lOk && rOk {
		return reduceBinOp(op, leftLit, rightLit)
	}

	return &binOpExpr{
		SampleExpr: left,
		RHS:        right,
		op:         op,
		opts:       opts,
	}
}

// Reduces a binary operation expression. A binop is reducible if both of its legs are literal expressions.
// This is because literals need match all labels, which is currently difficult to encode into StepEvaluators.
// Therefore, we ensure a binop can be reduced/simplified, maintaining the invariant that it does not have two literal legs.
func reduceBinOp(op string, left, right *literalExpr) *literalExpr {
	var res promql.Sample
	mergeBinOp(
		op,
		&promql.Sample{Point: promql.Point{V: left.value}},
		&promql.Sample{Point: promql.Point{V: right.value}},
		false,
		false,
		&res,
	)
	return &literalExpr{value: res.V}
}

type literalExpr struct {
	value float64
	implicit
}

func mustNewLiteralExpr(s string, invert bool) *literalExpr {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(newParseError(fmt.Sprintf("unable to parse literal as a float: %s", err.Error()), 0, 0))
	}

	if invert {
		n = -n
	}

	return &literalExpr{
		value: n,
	}
}

func (e *literalExpr) String() string {
	return fmt.Sprint(e.value)
}

// literlExpr impls SampleExpr & LogSelectorExpr mainly to reduce the need for more complicated typings
// to facilitate sum types. We'll be type switching when evaluating them anyways
// and they will only be present in binary operation legs.
func (e *literalExpr) Selector() LogSelectorExpr               { return e }
func (e *literalExpr) HasFilter() bool                         { return false }
func (e *literalExpr) Shardable() bool                         { return true }
func (e *literalExpr) Pipeline() (log.Pipeline, error)         { return log.NewNoopPipeline(), nil }
func (e *literalExpr) Matchers() []*labels.Matcher             { return nil }
func (e *literalExpr) Extractor() (log.SampleExtractor, error) { return nil, nil }

// helper used to impl Stringer for vector and range aggregations
// nolint:interfacer
func formatOperation(op string, grouping *grouping, params ...string) string {
	nonEmptyParams := make([]string, 0, len(params))
	for _, p := range params {
		if p != "" {
			nonEmptyParams = append(nonEmptyParams, p)
		}
	}

	var sb strings.Builder
	sb.WriteString(op)
	if grouping != nil {
		sb.WriteString(grouping.String())
	}
	sb.WriteString("(")
	sb.WriteString(strings.Join(nonEmptyParams, ","))
	sb.WriteString(")")
	return sb.String()
}

type labelReplaceExpr struct {
	left        SampleExpr
	dst         string
	replacement string
	src         string
	regex       string
	re          *regexp.Regexp

	implicit
}

func mustNewLabelReplaceExpr(left SampleExpr, dst, replacement, src, regex string) *labelReplaceExpr {
	re, err := regexp.Compile("^(?:" + regex + ")$")
	if err != nil {
		panic(newParseError(fmt.Sprintf("invalid regex in label_replace: %s", err.Error()), 0, 0))
	}
	return &labelReplaceExpr{
		left:        left,
		dst:         dst,
		replacement: replacement,
		src:         src,
		re:          re,
		regex:       regex,
	}
}

func (e *labelReplaceExpr) Selector() LogSelectorExpr {
	return e.left.Selector()
}

func (e *labelReplaceExpr) Extractor() (SampleExtractor, error) {
	return e.left.Extractor()
}

func (e *labelReplaceExpr) Shardable() bool {
	return false
}

func (e *labelReplaceExpr) String() string {
	var sb strings.Builder
	sb.Grow(64)
	sb.WriteString(OpLabelReplace)
	sb.WriteString("(")
	sb.WriteString(e.left.String())
	sb.WriteString(",")
	StringBuilderQuoteTo(&sb, e.dst)
	// sb.WriteString(strconv.Quote(e.dst))
	sb.WriteString(",")
	StringBuilderQuoteTo(&sb, e.replacement)
	// sb.WriteString(strconv.Quote(e.replacement))
	sb.WriteString(",")
	StringBuilderQuoteTo(&sb, e.src)
	// sb.WriteString(strconv.Quote(e.src))
	sb.WriteString(",")
	StringBuilderQuoteTo(&sb, e.regex)
	// sb.WriteString(strconv.Quote(e.regex))
	sb.WriteString(")")
	return sb.String()
}

const (
	lowerhex = "0123456789abcdef"
)

func StringBuilderQuoteTo(b *strings.Builder, s string) {
	_ = b.WriteByte('"')
	for i, r := range s {
		if 0x20 <= r && r <= 0x7E && r != '\\' && r != '"' {
			// fast path for common case
			_ = b.WriteByte(byte(r))
			continue
		}

		width := 1
		if r >= utf8.RuneSelf {
			width = utf8.RuneLen(r)
		}
		if r == utf8.RuneError && width == 1 {
			_, _ = b.WriteString(`\x`)
			_ = b.WriteByte(lowerhex[s[i]>>4])
			_ = b.WriteByte(lowerhex[s[i]&0xF])
			continue
		}
		{
			var runeTmp [utf8.UTFMax]byte
			if r == '"' || r == '\\' {
				_ = b.WriteByte('\\')
				_ = b.WriteByte(byte(r))
				continue
			}
			if strconv.IsPrint(r) {
				n := utf8.EncodeRune(runeTmp[:], r)
				_, _ = b.Write(runeTmp[:n])
				continue
			}
			switch r {
			case '\a':
				_, _ = b.WriteString(`\a`)
			case '\b':
				_, _ = b.WriteString(`\b`)
			case '\f':
				_, _ = b.WriteString(`\f`)
			case '\n':
				_, _ = b.WriteString(`\n`)
			case '\r':
				_, _ = b.WriteString(`\r`)
			case '\t':
				_, _ = b.WriteString(`\t`)
			case '\v':
				_, _ = b.WriteString(`\v`)
			default:
				switch {
				case r < ' ' || r == 0x7f:
					_, _ = b.WriteString(`\x`)
					_ = b.WriteByte(lowerhex[byte(r)>>4])
					_ = b.WriteByte(lowerhex[byte(r)&0xF])
				case !utf8.ValidRune(r):
					r = 0xFFFD
					fallthrough
				case r < 0x10000:
					_, _ = b.WriteString(`\u`)
					for s := 12; s >= 0; s -= 4 {
						_ = b.WriteByte(lowerhex[r>>uint(s)&0xF])
					}
				default:
					_, _ = b.WriteString(`\U`)
					for s := 28; s >= 0; s -= 4 {
						_ = b.WriteByte(lowerhex[r>>uint(s)&0xF])
					}
				}
			}
		}
	}
	_ = b.WriteByte('"')
}
