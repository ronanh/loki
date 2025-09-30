package logql

import (
	"testing"
)

func TestQueryType(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{"bad", "ddd", "", true},
		{"limited", `{app="foo"}`, QueryTypeLimited, false},
		{"limited multi label", `{app="foo" ,fuzz=~"foo"}`, QueryTypeLimited, false},
		{"limited with parser", `{app="foo" ,fuzz=~"foo"} | logfmt`, QueryTypeLimited, false},
		{"filter", `{app="foo"} |= "foo"`, QueryTypeFilter, false},
		{"filter string extracted label", `{app="foo"} | json | foo="a"`, QueryTypeFilter, false},
		{"filter duration", `{app="foo"} | json | duration > 5s`, QueryTypeFilter, false},
		{"metrics", `rate({app="foo"} |= "foo"[5m])`, QueryTypeMetric, false},
		{
			"metrics binary",
			`rate({app="foo"} |= "foo"[5m]) + count_over_time({app="foo"} |= "foo"[5m]) / rate({app="foo"} |= "foo"[5m]) `,
			QueryTypeMetric,
			false,
		},
		{"filters", `{app="foo"} |= "foo" |= "f" != "b"`, QueryTypeFilter, false},
		{
			"filters and labels filters",
			`{app="foo"} |= "foo" |= "f" != "b" | json | a > 5`,
			QueryTypeFilter,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := QueryType(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("QueryType() = %v, want %v", got, tt.want)
			}
		})
	}
}
