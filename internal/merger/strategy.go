package merger

import (
	"context"
	"encoding/json"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// StrategyInline: on flush, return batch to pipeline output (single pipeline merge + emit).
	StrategyInline = "inline"
	// StrategyStateStore: on flush, write merged state to cache; a separate periodic flow reads and publishes.
	StrategyStateStore = "state_store"
)

// FlushState is the merged state per device (vincode -> sensors).
type FlushState map[string]map[string]MetricSlot

// MetricSlot holds value and received_at for one sensor.
type MetricSlot struct {
	Value      any
	ReceivedAt int64
}

// FlushStrategy defines what to do when the merger flushes (every 2 min).
type FlushStrategy interface {
	// OnFlush is called with the current merged state. Return the batch to emit from this pipeline;
	// StateStore strategies return nil (they write to cache instead).
	OnFlush(ctx context.Context, state FlushState) (service.MessageBatch, error)
}

// InlineFlushStrategy returns the full batch so the pipeline output sends it to Kafka (Strategy 1).
type InlineFlushStrategy struct{}

func (InlineFlushStrategy) OnFlush(ctx context.Context, state FlushState) (service.MessageBatch, error) {
	produceAt := time.Now().UnixMilli()
	out := service.MessageBatch{}
	for vin, dev := range state {
		data := map[string]any{"id": vin}
		for sensor, slot := range dev {
			data[sensor] = map[string]any{
				"value":       slot.Value,
				"received_at": slot.ReceivedAt,
			}
		}
		payload := map[string]any{
			"num_of_data": 1,
			"data":        data,
			"produce_at":  produceAt,
		}
		msg := service.NewMessage(nil)
		msg.SetStructured(payload)
		msg.MetaSet("vincode", vin)
		out = append(out, msg)
	}
	return out, nil
}

// StateStoreFlushStrategy writes merged state to a Bento cache; returns nil batch (Strategy 2).
// A separate pipeline (generate 2m -> LatestStatePublisher -> Kafka) reads from cache and publishes.
type StateStoreFlushStrategy struct {
	CacheName string
	IndexKey  string
	Prefix    string
	Resources *service.Resources
}

const defaultIndexKey = "latest:__index__"
const defaultPrefix = "latest:"

func (s *StateStoreFlushStrategy) OnFlush(ctx context.Context, state FlushState) (service.MessageBatch, error) {
	if s.Resources == nil || s.CacheName == "" {
		return nil, nil
	}
	indexKey := s.IndexKey
	if indexKey == "" {
		indexKey = defaultIndexKey
	}
	prefix := s.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}

	vincodes := make([]string, 0, len(state))
	var setErr error
	err := s.Resources.AccessCache(ctx, s.CacheName, func(c service.Cache) {
		for vin, dev := range state {
			data := map[string]any{"id": vin}
			for sensor, slot := range dev {
				data[sensor] = map[string]any{
					"value":       slot.Value,
					"received_at": slot.ReceivedAt,
				}
			}
			payload := map[string]any{
				"num_of_data": 1,
				"data":        data,
				"produce_at":  time.Now().UnixMilli(),
			}
			b, err := json.Marshal(payload)
			if err != nil {
				setErr = err
				return
			}
			if err := c.Set(ctx, prefix+vin, b, nil); err != nil {
				setErr = err
				return
			}
			vincodes = append(vincodes, vin)
		}
		indexBytes, err := json.Marshal(vincodes)
		if err != nil {
			setErr = err
			return
		}
		if setErr == nil {
			setErr = c.Set(ctx, indexKey, indexBytes, nil)
		}
	})
	if err != nil {
		return nil, err
	}
	if setErr != nil {
		return nil, setErr
	}
	return nil, nil
}
