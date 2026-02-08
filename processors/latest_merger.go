package processors

import (
	"bethos/internal/model"
	"context"
	"encoding/json"
	"sync"
	"time"

	"bethos/internal/merger"

	"github.com/warpstreamlabs/bento/public/service"
)

// LatestMerger is a stateful processor that merges telemetry by device (vincode).
// On each Kafka message it merges into state. When it receives a flush trigger
// (message with _flush: true, e.g. from generate input every 2m), it delegates to FlushStrategy.
type LatestMerger struct {
	mu       sync.Mutex
	state    map[string]*DeviceState
	Strategy merger.FlushStrategy
}

type DeviceState struct {
	Metrics  map[string]model.MetricValue
	LastSeen int64
}

func (m *LatestMerger) Close(ctx context.Context) error {
	return nil
}

func (m *LatestMerger) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	obj, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	// Flush trigger
	if string(obj) == `{"_flush":true}` {
		return m.flush(ctx)
	}

	var payload model.Payload
	if err := json.Unmarshal(obj, &payload); err != nil {
		return nil, err
	}

	m.merge(payload)

	return nil, nil
}

func (m *LatestMerger) merge(p model.Payload) {
	vin := p.Data.ID
	if vin == "" {
		return
	}

	now := time.Now().UnixMilli()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state == nil {
		m.state = make(map[string]*DeviceState)
	}

	dev := m.state[vin]
	if dev == nil {
		dev = &DeviceState{
			Metrics:  make(map[string]model.MetricValue),
			LastSeen: now,
		}
		m.state[vin] = dev
	}

	for sensor, metric := range p.Data.Metrics {
		if metric.ReceivedAt >= dev.Metrics[sensor].ReceivedAt {
			dev.Metrics[sensor] = metric
		}
	}

	dev.LastSeen = now
}

func (m *LatestMerger) flush(ctx context.Context) (service.MessageBatch, error) {
	now := time.Now().UnixMilli()
	const deviceTTL = int64(10 * time.Minute / time.Millisecond)

	m.mu.Lock()

	snapshot := make(map[string]map[string]model.MetricValue)
	for vin, dev := range m.state {

		// TTL eviction
		if now-dev.LastSeen > deviceTTL {
			delete(m.state, vin)
			continue
		}

		copyDev := make(map[string]model.MetricValue, len(dev.Metrics))
		for k, v := range dev.Metrics {
			copyDev[k] = v
		}
		snapshot[vin] = copyDev
	}

	m.mu.Unlock()

	return m.Strategy.OnFlush(ctx, snapshot)
}
