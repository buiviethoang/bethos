package processors

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"bethos/internal/model"
	"bethos/internal/merger"

	"github.com/warpstreamlabs/bento/public/service"
)

// BenchmarkLatestMerger_Process_SameVIN merges many payloads for the same VIN.
func BenchmarkLatestMerger_Process_SameVIN(b *testing.B) {
	ctx := context.Background()
	m := &LatestMerger{Strategy: &merger.LogCompactedFlushStrategy{}}
	body := []byte(`{"num_of_data":1,"data":{"id":"VIN1","s1":{"value":"v","received_at":100}},"produced_at":1}`)
	msg := service.NewMessage(nil)
	msg.SetBytes(body)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Process(ctx, msg)
	}
}

// BenchmarkLatestMerger_Process_ManyVINs merges one payload per distinct VIN (round-robin).
func BenchmarkLatestMerger_Process_ManyVINs(b *testing.B) {
	ctx := context.Background()
	m := &LatestMerger{Strategy: &merger.LogCompactedFlushStrategy{}}
	const numVINs = 1000
	msgs := make([]*service.Message, numVINs)
	for i := 0; i < numVINs; i++ {
		p := model.Payload{
			NumOfData:  1,
			Data:       model.Data{ID: fmt.Sprintf("VIN%d", i), Metrics: map[string]model.MetricValue{"s1": {Value: "v", ReceivedAt: int64(100 + i)}}},
			ProducedAt: 1,
		}
		raw, _ := json.Marshal(p)
		msg := service.NewMessage(nil)
		msg.SetBytes(raw)
		msgs[i] = msg
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Process(ctx, msgs[i%numVINs])
	}
}

// BenchmarkLatestMerger_Flush_LargeState flushes after building a large state (many VINs).
func BenchmarkLatestMerger_Flush_LargeState(b *testing.B) {
	ctx := context.Background()
	m := &LatestMerger{Strategy: &merger.LogCompactedFlushStrategy{}}
	const numVINs = 5000
	for i := 0; i < numVINs; i++ {
		vin := fmt.Sprintf("VIN%d", i)
		m.merge(model.Payload{
			NumOfData: 1,
			Data: model.Data{
				ID: vin,
				Metrics: map[string]model.MetricValue{
					"sensor_a": {Value: "x", ReceivedAt: int64(100 + i)},
					"sensor_b": {Value: 42, ReceivedAt: int64(100 + i)},
				},
			},
			ProducedAt: 1,
		})
	}
	flushMsg := service.NewMessage([]byte(`{"_flush":true}`))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Process(ctx, flushMsg)
	}
}
