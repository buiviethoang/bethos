package processors

import (
	"context"
	"fmt"
	"sync"

	"bethos/internal/merger"

	"github.com/warpstreamlabs/bento/public/service"
)

// LatestMerger is a stateful processor that merges telemetry by device (vincode).
// On each Kafka message it merges into state. When it receives a flush trigger
// (message with _flush: true, e.g. from generate input every 2m), it delegates to FlushStrategy.
type LatestMerger struct {
	mu       sync.Mutex
	state    map[string]map[string]metricSlot // vincode -> sensor_name -> { value, received_at }
	Strategy merger.FlushStrategy
}

type metricSlot struct {
	Value      any
	ReceivedAt int64
}

func (m *LatestMerger) Close(ctx context.Context) error {
	return nil
}

func (m *LatestMerger) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	obj, err := msg.AsStructured()
	if err != nil {
		return nil, err
	}

	in, ok := obj.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("latest_merger: expected map, got %T", obj)
	}

	// Flush trigger from generate input (interval 2m)
	if flush, _ := in["_flush"].(bool); flush {
		return m.flush(ctx)
	}

	// Merge incoming aggregated message into state
	m.merge(in)
	// Drop this message (do not forward)
	return nil, nil
}

func (m *LatestMerger) merge(in map[string]any) {
	data, ok := in["data"].(map[string]any)
	if !ok {
		return
	}

	vin, _ := data["id"].(string)
	if vin == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		m.state = make(map[string]map[string]metricSlot)
	}
	dev := m.state[vin]
	if dev == nil {
		dev = make(map[string]metricSlot)
		m.state[vin] = dev
	}

	for k, v := range data {
		if k == "id" {
			continue
		}
		slot := toSlot(v)
		if slot.ReceivedAt >= dev[k].ReceivedAt {
			dev[k] = slot
		}
	}
}

func toSlot(v any) metricSlot {
	s := metricSlot{}
	if ma, ok := v.(map[string]any); ok {
		s.Value = ma["value"]
		if ra, ok := ma["received_at"]; ok {
			switch t := ra.(type) {
			case int64:
				s.ReceivedAt = t
			case float64:
				s.ReceivedAt = int64(t)
			case int:
				s.ReceivedAt = int64(t)
			}
		}
	}
	return s
}

func (m *LatestMerger) flush(ctx context.Context) (service.MessageBatch, error) {
	m.mu.Lock()
	snapshot := make(map[string]map[string]merger.MetricSlot, len(m.state))
	for vin, dev := range m.state {
		devCopy := make(map[string]merger.MetricSlot, len(dev))
		for k, v := range dev {
			devCopy[k] = merger.MetricSlot{Value: v.Value, ReceivedAt: v.ReceivedAt}
		}
		snapshot[vin] = devCopy
	}
	m.mu.Unlock()

	if m.Strategy != nil {
		return m.Strategy.OnFlush(ctx, snapshot)
	}
	// Default: inline (build batch and return)
	return merger.InlineFlushStrategy{}.OnFlush(ctx, snapshot)
}
