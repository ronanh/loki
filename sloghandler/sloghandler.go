package sloghandler

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/api/trace"
)

func New(inner slog.Handler) slog.Handler {
	return &handlerImpl{inner}
}

type handlerImpl struct {
	slog.Handler
}

func (ch *handlerImpl) Handle(ctx context.Context, r slog.Record) error {
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	if spanCtx.HasTraceID() {
		traceID := spanCtx.TraceID.String()
		r.Add("traceID", slog.StringValue(traceID))
	}

	return ch.Handler.Handle(ctx, r)
}
