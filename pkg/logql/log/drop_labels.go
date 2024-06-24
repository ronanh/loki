package log

import (
	"github.com/prometheus/prometheus/pkg/labels"
)

type DropLabels struct {
	dropLabels []DropLabel
}

type DropLabel struct {
	Matcher *labels.Matcher
	Name    string
}

func NewDropLabel(matcher *labels.Matcher, name string) DropLabel {
	return DropLabel{
		Matcher: matcher,
		Name:    name,
	}
}

func NewDropLabels(dropLabels []DropLabel) *DropLabels {
	return &DropLabels{dropLabels: dropLabels}
}

func (dl *DropLabels) Process(line []byte, lbls *LabelsBuilder) ([]byte, bool) {
	for _, dropLabel := range dl.dropLabels {
		if dropLabel.Matcher != nil {
			switch dropLabel.Name {
			case ErrorLabel:
				if dropLabel.Matcher.Matches(lbls.GetErr()) {
					lbls.SetErr("")
				}
			default:
				value, _ := lbls.Get(dropLabel.Name)
				if dropLabel.Matcher.Matches(value) {
					lbls.Del(dropLabel.Name)
				}
			}
		} else {
			switch dropLabel.Name {
			case ErrorLabel:
				lbls.SetErr("")
			default:
				if _, ok := lbls.Get(dropLabel.Name); ok {
					lbls.Del(dropLabel.Name)
				}
			}
		}
	}
	return line, true
}

func (dl *DropLabels) RequiredLabelNames() []string {
	return []string{}
}
