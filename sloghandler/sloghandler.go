package sloghandler

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

func New(inner slog.Handler) slog.Handler {
	return &handlerImpl{inner}
}

type handlerImpl struct {
	slog.Handler
}

func (ch *handlerImpl) Handle(ctx context.Context, r slog.Record) error {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		traceID := spanCtx.TraceID().String()
		r.Add("traceID", slog.StringValue(traceID))
	}

	return ch.Handler.Handle(ctx, r)
}

func (ch *handlerImpl) WithAttrs([]slog.Attr) slog.Handler {
	return ch.clone()
}

func (ch *handlerImpl) clone() *handlerImpl {
	clone := *ch
	return &clone
}
