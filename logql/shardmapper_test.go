package logql

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
)

func TestShardedStringer(t *testing.T) {
	for _, tc := range []struct {
		in  Expr
		out string
	}{
		{
			in: &ConcatLogSelectorExpr{
				DownstreamLogSelectorExpr: DownstreamLogSelectorExpr{
					shard: &astmapper.ShardAnnotation{
						Shard: 0,
						Of:    2,
					},
					LogSelectorExpr: &matchersExpr{
						matchers: []*labels.Matcher{
							mustNewMatcher(labels.MatchEqual, "foo", "bar"),
						},
					},
				},
				next: &ConcatLogSelectorExpr{
					DownstreamLogSelectorExpr: DownstreamLogSelectorExpr{
						shard: &astmapper.ShardAnnotation{
							Shard: 1,
							Of:    2,
						},
						LogSelectorExpr: &matchersExpr{
							matchers: []*labels.Matcher{
								mustNewMatcher(labels.MatchEqual, "foo", "bar"),
							},
						},
					},
					next: nil,
				},
			},
			out: `downstream<{foo="bar"}, shard=0_of_2> ++ downstream<{foo="bar"}, shard=1_of_2>`,
		},
	} {
		t.Run(tc.out, func(t *testing.T) {
			require.Equal(t, tc.out, tc.in.String())
		})
	}
}
