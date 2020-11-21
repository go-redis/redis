package internal

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8/internal/proto"
	"github.com/go-redis/redis/v8/internal/util"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/trace"
)

func Sleep(ctx context.Context, dur time.Duration) error {
	return WithSpan(ctx, "time.Sleep", func(ctx context.Context, span trace.Span) error {
		t := time.NewTimer(dur)
		defer t.Stop()

		select {
		case <-t.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func ToLower(s string) string {
	if isLower(s) {
		return s
	}

	b := make([]byte, len(s))
	for i := range b {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return util.BytesToString(b)
}

func isLower(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

func WithSpan(ctx context.Context, name string, fn func(context.Context, trace.Span) error) error {
	if span := trace.SpanFromContext(ctx); !span.IsRecording() {
		return fn(ctx, span)
	}

	ctx, span := global.Tracer("github.com/go-redis/redis").Start(ctx, name)
	defer span.End()

	return fn(ctx, span)
}

func RecordError(ctx context.Context, err error) error {
	if err != proto.Nil {
		trace.SpanFromContext(ctx).RecordError(ctx, err)
	}
	return err
}

// CountWriteError increments the redisError instrument
// adding a write label to the count. The error message
// is also included as a label.
func CountWriteError(ctx context.Context, err error) {
	if err != proto.Nil {
		redisErrors.Add(
			ctx,
			1,
			dbErrorMessage(err.Error()),
			dbErrorSourceWrite(),
		)
	}
}

// CountReadError increments the redisError instrument
// adding a read label to the count. The error message
// is also included as a label.
func CountReadError(ctx context.Context, err error) {
	if err != proto.Nil {
		redisErrors.Add(
			ctx,
			1,
			dbErrorMessage(err.Error()),
			dbErrorSourceRead(),
		)
	}
}
