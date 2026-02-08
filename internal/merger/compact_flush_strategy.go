package merger

import (
	"context"
	"time"

	"bethos/internal/model"

	"github.com/warpstreamlabs/bento/public/service"
)

type LogCompactedFlushStrategy struct{}

func (LogCompactedFlushStrategy) Close(ctx context.Context) error {
	return nil
}

func (LogCompactedFlushStrategy) OnFlush(
	ctx context.Context,
	state FlushState,
) (service.MessageBatch, error) {

	var batch service.MessageBatch
	now := time.Now().UnixMilli()

	for vin, dev := range state {

		payload := model.Payload{
			NumOfData: 1,
			Data: model.Data{
				ID:      vin,
				Metrics: dev,
			},
			ProducedAt: now,
		}

		msg := service.NewMessage(nil)
		msg.SetStructured(payload)

		// This makes Kafka partition by VIN
		msg.MetaSet("kafka_key", vin)
		msg.MetaSet("vincode", vin)

		batch = append(batch, msg)
	}

	return batch, nil
}
