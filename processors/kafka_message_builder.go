package processors

import (
	"context"
	"fmt"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type KafkaBuilder struct{}

func (k *KafkaBuilder) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
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

	payload := map[string]any{
		"id": vincode,
	}
	for key, val := range metrics {
		payload[key] = val
	}

	msg.SetStructured(map[string]any{
		"num_of_data": 1,
		"data":        []any{payload},
		"produce_at":  time.Now().UnixMilli(),
	})

	msg.MetaSet("vincode", vincode)

	return service.MessageBatch{msg}, nil
}
