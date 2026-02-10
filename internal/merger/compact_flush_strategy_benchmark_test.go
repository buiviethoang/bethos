package merger

import (
	"context"
	"fmt"
	"testing"

	"bethos/internal/model"
)

// BenchmarkLogCompactedFlushStrategy_OnFlush_LargeState benchmarks OnFlush with many VINs.
func BenchmarkLogCompactedFlushStrategy_OnFlush_LargeState(b *testing.B) {
	ctx := context.Background()
	s := LogCompactedFlushStrategy{}
	const numVINs = 5000
	state := make(FlushState, numVINs)
	for i := 0; i < numVINs; i++ {
		state[fmt.Sprintf("VIN%d", i)] = map[string]model.MetricValue{
			"sensor_a": {Value: "x", ReceivedAt: int64(100 + i)},
			"sensor_b": {Value: 42, ReceivedAt: int64(100 + i)},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.OnFlush(ctx, state)
	}
}
