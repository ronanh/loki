package logproto

import (
	"testing"
	time "time"

	"github.com/stretchr/testify/require"
)

var (
	now           = time.Now().UTC()
	line          = `level=info ts=2019-12-12T15:00:08.325Z caller=compact.go:441 component=tsdb msg="compact blocks" count=3 mint=1576130400000 maxt=1576152000000 ulid=01DVX9ZHNM71GRCJS7M34Q0EV7 sources="[01DVWNC6NWY1A60AZV3Z6DGS65 01DVWW7XXX75GHA6ZDTD170CSZ 01DVX33N5W86CWJJVRPAVXJRWJ]" duration=2.897213221s`
	streamAdapter = StreamAdapter{
		Labels: `{job="foobar", cluster="foo-central1", namespace="bar", container_name="buzz"}`,
		Entries: []EntryAdapter{
			{now, line},
			{now.Add(1 * time.Second), line},
			{now.Add(2 * time.Second), line},
			{now.Add(3 * time.Second), line},
		},
	}
)

func TestStreamAdapter(t *testing.T) {
	avg := testing.AllocsPerRun(200, func() {
		b, err := streamAdapter.Marshal()
		require.NoError(t, err)

		var adapter StreamAdapter
		err = adapter.Unmarshal(b)
		require.NoError(t, err)

		require.Equal(t, streamAdapter, adapter)
	})
	t.Log("avg allocs per run:", avg)
}

func BenchmarkStreamAdapter(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		by, err := streamAdapter.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		var adapter StreamAdapter
		err = adapter.Unmarshal(by)
		if err != nil {
			b.Fatal(err)
		}
	}
}
