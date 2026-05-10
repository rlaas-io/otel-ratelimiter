package ratelimiterprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestProcessorTelemetry_RecordMethods(t *testing.T) {
	tel, err := newProcessorTelemetry(noop.NewMeterProvider(), "logs")
	require.NoError(t, err)
	require.NotNil(t, tel)

	ctx := context.Background()
	tel.recordReceived(ctx)
	tel.recordDropped(ctx, "rate_limited")
	tel.recordAllowed(ctx)
	tel.recordShadow(ctx)
	tel.recordError(ctx)
	tel.recordReload()
}
