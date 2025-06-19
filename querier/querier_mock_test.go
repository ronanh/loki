package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/util"
	grpc_metadata "google.golang.org/grpc/metadata"
)

// tailClientMock is mockable version of Querier_TailClient
type tailClientMock struct {
	util.ExtendedMock
	logproto.Querier_TailClient
	recvTrigger chan time.Time
}

func newTailClientMock() *tailClientMock {
	return &tailClientMock{
		recvTrigger: make(chan time.Time, 10),
	}
}

func (c *tailClientMock) Recv() (*logproto.TailResponse, error) {
	args := c.Called()
	return args.Get(0).(*logproto.TailResponse), args.Error(1)
}

func (c *tailClientMock) Header() (grpc_metadata.MD, error) {
	return nil, nil
}

func (c *tailClientMock) Trailer() grpc_metadata.MD {
	return nil
}

func (c *tailClientMock) CloseSend() error {
	return nil
}

func (c *tailClientMock) Context() context.Context {
	return context.Background()
}

func (c *tailClientMock) SendMsg(m interface{}) error {
	return nil
}

func (c *tailClientMock) RecvMsg(m interface{}) error {
	return nil
}

func (c *tailClientMock) mockRecvWithTrigger(response *logproto.TailResponse) *tailClientMock {
	c.On("Recv").WaitUntil(c.recvTrigger).Return(response, nil)

	return c
}

// triggerRecv triggers the Recv() mock to return from the next invocation
// or from the current invocation if was already called and waiting for the
// trigger. This method works if and only if the Recv() has been mocked with
// mockRecvWithTrigger().
func (c *tailClientMock) triggerRecv() {
	c.recvTrigger <- time.Now()
}

// mockStreamIterator returns an iterator with 1 stream and quantity entries,
// where entries timestamp and line string are constructed as sequential numbers
// starting at from
func mockStreamIterator(from int, quantity int) iter.EntryIterator {
	return iter.NewStreamIterator(mockStream(from, quantity))
}

// mockStream return a stream with quantity entries, where entries timestamp and
// line string are constructed as sequential numbers starting at from
func mockStream(from int, quantity int) logproto.Stream {
	return mockStreamWithLabels(from, quantity, `{type="test"}`)
}

func mockStreamWithLabels(from int, quantity int, labels string) logproto.Stream {
	entries := make([]logproto.Entry, 0, quantity)

	for i := from; i < from+quantity; i++ {
		entries = append(entries, logproto.Entry{
			Timestamp: time.Unix(int64(i), 0),
			Line:      fmt.Sprintf("line %d", i),
		})
	}

	return logproto.Stream{
		Entries: entries,
		Labels:  labels,
	}
}
