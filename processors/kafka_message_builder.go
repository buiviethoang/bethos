package processors

import (
	"context"
	"fmt"

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

	payload, ok := obj.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid structure")
	}

	data := payload["data"].(map[string]any)
	vincode := data["id"].(string)

	msg.MetaSet("vincode", vincode)

	return service.MessageBatch{msg}, nil
}
