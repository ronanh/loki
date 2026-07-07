package log

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/ronanh/loki/util"
)

// NoopStage is a stage that doesn't process a log line.
var NoopStage Stage = &noopStage{}

// Pipeline can create pipelines for each log stream.
type Pipeline interface {
	ForStream(labels labels.Labels) StreamPipeline
}

// StreamPipeline transform and filter log lines and labels.
// A StreamPipeline never mutate the received line.
//
// Process receives, per line, the optional deltaLabels (additive per-line
// labels relative to the stream's labels, decoded from the storage
// attrDeltas column) and deltaHash, the canonical hash of the line's
// effective label set (stream ⊕ delta) as stored in the attrHashes column;
// 0 = unavailable, recompute. Lines without per-line labels pass
// (labels.EmptyLabels(), 0).
type StreamPipeline interface {
	// BaseLabels returns the stream's labels result, unaffected by any line.
	BaseLabels() LabelsResult
	Process(
		ts int64,
		line []byte,
		deltaLabels labels.Labels,
		deltaHash uint64,
	) (resultLine []byte, resultLabels LabelsResult, skip bool)
	ProcessString(
		ts int64,
		line string,
		deltaLabels labels.Labels,
		deltaHash uint64,
	) (resultLine string, resultLabels LabelsResult, skip bool)
	// ReferencedDeltaLabels reports whether the pipeline's line
	// filtering/transformation can depend on the deltaLabels contents.
	// When false, callers may skip decoding the per-line delta column for
	// processing purposes (the deltas still flow into the result labels) —
	// e.g. pass stored delta blobs through wholesale.
	ReferencedDeltaLabels() bool
}

// Stage is a single step of a Pipeline.
// A Stage implementation should never mutate the line passed, but instead either
// return the line unchanged or allocate a new line.
type Stage interface {
	Process(ts int64, line []byte, lbs *LabelsBuilder) ([]byte, bool)
	RequiredLabelNames() []string
}

// NewNoopPipeline creates a pipelines that does not process anything and returns log streams as is.
func NewNoopPipeline() Pipeline {
	return &noopPipeline{
		cache:       map[uint64]*noopStreamPipeline{},
		baseBuilder: NewBaseLabelsBuilder(),
	}
}

type noopPipeline struct {
	cache map[uint64]*noopStreamPipeline
	// baseBuilder serves the delta-carrying lines: even a noop pipeline must
	// surface per-line labels in its results.
	baseBuilder *BaseLabelsBuilder
}

// IsNoopPipeline tells if a pipeline is a Noop.
func IsNoopPipeline(p Pipeline) bool {
	_, ok := p.(*noopPipeline)
	return ok
}

type noopStreamPipeline struct {
	base    LabelsResult
	builder *LabelsBuilder
}

func (n *noopStreamPipeline) BaseLabels() LabelsResult { return n.base }

func (n *noopStreamPipeline) ReferencedDeltaLabels() bool { return false }

func (n *noopStreamPipeline) Process(
	_ int64,
	line []byte,
	deltaLabels labels.Labels,
	deltaHash uint64,
) ([]byte, LabelsResult, bool) {
	if deltaLabels.IsEmpty() {
		return line, n.base, true
	}
	n.builder.Reset()
	n.builder.SetDeltaLabels(deltaLabels, deltaHash)
	return line, n.builder.LabelsResult(), true
}

func (n *noopStreamPipeline) ProcessString(
	ts int64,
	line string,
	deltaLabels labels.Labels,
	deltaHash uint64,
) (string, LabelsResult, bool) {
	_, lr, ok := n.Process(ts, nil, deltaLabels, deltaHash)
	return line, lr, ok
}

func (n *noopPipeline) ForStream(lbs labels.Labels) StreamPipeline {
	// StableHash keeps the same hash values as the historic labels.Hash() and
	// stays consistent with the HashWithoutLabels-based pipeline hashes.
	h := labels.StableHash(lbs)
	if cached, ok := n.cache[h]; ok {
		return cached
	}
	sp := &noopStreamPipeline{
		base:    NewLabelsResult(lbs, h),
		builder: n.baseBuilder.ForLabels(lbs, h),
	}
	n.cache[h] = sp
	return sp
}

type noopStage struct{}

func (noopStage) Process(_ int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	return line, true
}
func (noopStage) RequiredLabelNames() []string { return []string{} }

type StageFunc struct {
	process        func(ts int64, line []byte, lbs *LabelsBuilder) ([]byte, bool)
	requiredLabels []string
	// lineOnly marks stages whose outcome depends only on the line content
	// (never on the labels builder) — e.g. line filters. Used to compute
	// ReferencedDeltaLabels.
	lineOnly bool
}

func (fn StageFunc) Process(ts int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	return fn.process(ts, line, lbs)
}

func (fn StageFunc) RequiredLabelNames() []string {
	if fn.requiredLabels == nil {
		return []string{}
	}
	return fn.requiredLabels
}

// stageIsLineOnly reports whether a stage is known to neither read nor write
// labels. Conservative: unknown stage types count as label-referencing.
func stageIsLineOnly(s Stage) bool {
	switch st := s.(type) {
	case *noopStage:
		return true
	case StageFunc:
		return st.lineOnly
	}
	return false
}

func stagesAreLineOnly(stages []Stage) bool {
	for _, s := range stages {
		if !stageIsLineOnly(s) {
			return false
		}
	}
	return true
}

// pipeline is a combinations of multiple stages.
// It can also be reduced into a single stage for convenience.
type pipeline struct {
	stages      []Stage
	baseBuilder *BaseLabelsBuilder

	streamPipelines map[uint64]StreamPipeline

	referencedDeltas bool
}

// NewPipeline creates a new pipeline for a given set of stages.
func NewPipeline(stages []Stage) Pipeline {
	if len(stages) == 0 {
		return NewNoopPipeline()
	}
	return &pipeline{
		stages:           stages,
		baseBuilder:      NewBaseLabelsBuilder(),
		streamPipelines:  make(map[uint64]StreamPipeline),
		referencedDeltas: !stagesAreLineOnly(stages),
	}
}

type streamPipeline struct {
	stages           []Stage
	builder          *LabelsBuilder
	referencedDeltas bool
}

func (p *pipeline) ForStream(labels labels.Labels) StreamPipeline {
	hash := p.baseBuilder.Hash(labels)
	if res, ok := p.streamPipelines[hash]; ok {
		return res
	}

	res := &streamPipeline{
		stages:           p.stages,
		builder:          p.baseBuilder.ForLabels(labels, hash),
		referencedDeltas: p.referencedDeltas,
	}
	p.streamPipelines[hash] = res
	return res
}

func (p *streamPipeline) BaseLabels() LabelsResult { return p.builder.BaseLabels() }

func (p *streamPipeline) ReferencedDeltaLabels() bool { return p.referencedDeltas }

func (p *streamPipeline) Process(
	ts int64,
	line []byte,
	deltaLabels labels.Labels,
	deltaHash uint64,
) ([]byte, LabelsResult, bool) {
	var ok bool
	p.builder.Reset()
	p.builder.SetDeltaLabels(deltaLabels, deltaHash)
	for _, s := range p.stages {
		line, ok = s.Process(ts, line, p.builder)
		if !ok {
			return nil, nil, false
		}
	}
	return line, p.builder.LabelsResult(), true
}

func (p *streamPipeline) ProcessString(
	ts int64,
	line string,
	deltaLabels labels.Labels,
	deltaHash uint64,
) (string, LabelsResult, bool) {
	// Stages only read from the line.
	lb := unsafeGetBytes(line)
	lb, lr, ok := p.Process(ts, lb, deltaLabels, deltaHash)
	// either the line is unchanged and we can just send back the same string.
	// or we created a new buffer for it in which case it is still safe to avoid the string(byte)
	// copy.
	return unsafeGetString(lb), lr, ok
}

// ReduceStages reduces multiple stages into one.
func ReduceStages(stages []Stage) Stage {
	if len(stages) == 0 {
		return NoopStage
	}
	var requiredLabelNames []string
	for _, s := range stages {
		requiredLabelNames = append(requiredLabelNames, s.RequiredLabelNames()...)
	}
	return StageFunc{
		process: func(ts int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
			var ok bool
			for _, p := range stages {
				line, ok = p.Process(ts, line, lbs)
				if !ok {
					return nil, false
				}
			}
			return line, true
		},
		requiredLabels: requiredLabelNames,
		lineOnly:       stagesAreLineOnly(stages),
	}
}

func unsafeGetBytes(s string) []byte {
	return util.StrToBytes(s)
}

func unsafeGetString(buf []byte) string {
	return util.BytesToStr(buf)
}
