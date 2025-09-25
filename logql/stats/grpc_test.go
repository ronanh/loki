package stats

import (
	"context"
	"io"
	"log"
	"net"
	"testing"

	"github.com/ronanh/loki/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var (
	lis    *bufconn.Listener
	server *grpc.Server
)

func init() {
	lis = bufconn.Listen(bufSize)
	server = grpc.NewServer()
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestCollectTrailer(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	ing := ingesterFn(func(s grpc.ServerStream) error {
		ingCtx := NewContext(s.Context())
		defer SendAsTrailer(ingCtx, s)
		GetIngesterData(ingCtx).TotalChunksMatched++
		GetIngesterData(ingCtx).TotalBatches = +2
		GetIngesterData(ingCtx).TotalLinesSent = +3
		GetChunkData(ingCtx).HeadChunkBytes++
		GetChunkData(ingCtx).HeadChunkLines++
		GetChunkData(ingCtx).DecompressedBytes++
		GetChunkData(ingCtx).DecompressedLines++
		GetChunkData(ingCtx).CompressedBytes++
		GetChunkData(ingCtx).TotalDuplicates++
		return nil
	})
	model.RegisterQuerierServer(server, ing)
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	ingClient := model.NewQuerierClient(conn)

	ctx = NewContext(ctx)

	// query the ingester twice once for logs , once for samples.
	clientStream, err := ingClient.Query(ctx, &model.QueryRequest{}, CollectTrailer(ctx))
	if err != nil {
		t.Fatal(err)
	}
	_, err = clientStream.Recv()
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	clientSamples, err := ingClient.QuerySample(
		ctx,
		&model.SampleQueryRequest{},
		CollectTrailer(ctx),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = clientSamples.Recv()
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	err = clientSamples.CloseSend()
	if err != nil {
		t.Fatal(err)
	}
	res := decodeTrailers(ctx)
	require.Equal(t, int32(2), res.Ingester.TotalReached)
	require.Equal(t, int64(2), res.Ingester.TotalChunksMatched)
	require.Equal(t, int64(4), res.Ingester.TotalBatches)
	require.Equal(t, int64(6), res.Ingester.TotalLinesSent)
	require.Equal(t, int64(2), res.Ingester.HeadChunkBytes)
	require.Equal(t, int64(2), res.Ingester.HeadChunkLines)
	require.Equal(t, int64(2), res.Ingester.DecompressedBytes)
	require.Equal(t, int64(2), res.Ingester.DecompressedLines)
	require.Equal(t, int64(2), res.Ingester.CompressedBytes)
	require.Equal(t, int64(2), res.Ingester.TotalDuplicates)
}

type ingesterFn func(grpc.ServerStream) error

func (i ingesterFn) Query(_ *model.QueryRequest, s model.Querier_QueryServer) error {
	return i(s)
}

func (ingesterFn) Label(context.Context, *model.LabelRequest) (*model.LabelResponse, error) {
	return nil, nil
}

func (ingesterFn) Series(
	context.Context,
	*model.SeriesRequest,
) (*model.SeriesResponse, error) {
	return nil, nil
}
