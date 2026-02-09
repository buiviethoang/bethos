package processors

import (
	"bethos/internal/model"
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"bethos/internal/merger"

	"github.com/warpstreamlabs/bento/public/service"
)

const logPrefix = "[latest_merger]"

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

// isFlushTrigger parses JSON and returns true if the message is a flush trigger (e.g. {"_flush": true}).
// Tolerates whitespace and key order so generate input is reliable.
func isFlushTrigger(obj []byte) bool {
	var v struct {
		Flush *bool `json:"_flush"`
	}
	if err := json.Unmarshal(obj, &v); err != nil {
		return false
	}
	return v.Flush != nil && *v.Flush
}

func (m *LatestMerger) Close(ctx context.Context) error {
	// Graceful shutdown: run flush for observability (batch cannot be emitted from Close in Bento)
	_, err := m.flush(ctx)
	if err != nil {
		log.Printf("%s event=shutdown_flush error=%v", logPrefix, err)
		return err
	}
	log.Printf("%s event=shutdown_flush note=data_not_emitted_on_shutdown", logPrefix)
	return nil
}

func (m *LatestMerger) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	obj, err := msg.AsBytes()
	if err != nil {
		log.Printf("%s event=error error=as_bytes err=%v", logPrefix, err)
		return nil, err
	}

	// Flush trigger (parse JSON so {"_flush": true} with any whitespace is accepted)
	if isFlushTrigger(obj) {
		return m.flush(ctx)
	}

	var payload model.Payload
	if err := json.Unmarshal(obj, &payload); err != nil {
		log.Printf("%s event=error error=unmarshal err=%v", logPrefix, err)
		return nil, err
	}

	if payload.Data.ID == "" {
		log.Printf("%s event=skip reason=empty_vin hint=payload_must_have_data.id", logPrefix)
		return nil, nil
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
	start := time.Now()
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

	vinCount := len(snapshot)
	batch, err := m.Strategy.OnFlush(ctx, snapshot)
	durationMs := time.Since(start).Milliseconds()
	msgCount := 0
	if batch != nil {
		msgCount = len(batch)
	}
	log.Printf("%s event=flush flush_duration_ms=%d vin_count=%d message_count=%d",
		logPrefix, durationMs, vinCount, msgCount)
	if err != nil {
		log.Printf("%s event=flush_error error=%v", logPrefix, err)
		return batch, err
	}
	return batch, nil
}
