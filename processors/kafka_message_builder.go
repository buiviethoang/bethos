package processors

import (
	"context"
	"fmt"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type KafkaBuilder struct{}

func (k *KafkaBuilder) Close(ctx context.Context) error {
	return nil
}

func (k *KafkaBuilder) Process(
	ctx context.Context,
	msg *service.Message,
) (service.MessageBatch, error) {

	obj, err := msg.AsStructured()
	if err != nil {
		return nil, err
	}

	in, ok := obj.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid message structure")
	}

	vincode, _ := in["vincode"].(string)
	metrics, _ := in["metrics"].(map[string]any)

	// data is a single object: id + each sensor as key with { value, received_at }
	data := map[string]any{
		"id": vincode,
	}
	for key, val := range metrics {
		data[key] = normalizeMetricValue(val)
	}

	produceAt := time.Now().UnixMilli()
	msg.SetStructured(map[string]any{
		"num_of_data": 1,
		"data":        data,
		"produce_at":  produceAt,
	})

	msg.MetaSet("vincode", vincode)

	return service.MessageBatch{msg}, nil
}

// normalizeMetricValue ensures value is { "value": ..., "received_at": ... } for Kafka spec.
func normalizeMetricValue(val any) map[string]any {
	if m, ok := val.(map[string]any); ok {
		var value any
		var receivedAt int64
		if v, ok := m["value"]; ok {
			value = v
		}
		if v, ok := m["received_at"]; ok {
			switch t := v.(type) {
			case int64:
				receivedAt = t
			case float64:
				receivedAt = int64(t)
			case int:
				receivedAt = int64(t)
			}
		}
		return map[string]any{"value": value, "received_at": receivedAt}
	}
	return map[string]any{"value": val, "received_at": int64(0)}
}
