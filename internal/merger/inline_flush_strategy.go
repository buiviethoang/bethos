package merger

import (
	"bethos/internal/model"
	"context"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type InlineFlushStrategy struct{}

func (InlineFlushStrategy) Close(ctx context.Context) error {
	return nil
}

func (InlineFlushStrategy) OnFlush(ctx context.Context, state FlushState) (service.MessageBatch, error) {
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
		msg.MetaSet("vincode", vin)

		batch = append(batch, msg)
	}

	return batch, nil
}
