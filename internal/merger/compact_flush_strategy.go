package merger

import (
	"context"
	"time"

	"bethos/internal/model"

	"github.com/warpstreamlabs/bento/public/service"
)

// LogCompactedFlushStrategy flushes merged state to Kafka. BatchSize controls how many devices per message (1 = one message per device).
type LogCompactedFlushStrategy struct {
	BatchSize int // if > 1, emit PayloadBatch with up to BatchSize devices per message to reduce network I/O
}

func (s LogCompactedFlushStrategy) Close(ctx context.Context) error {
	return nil
}

func (s LogCompactedFlushStrategy) OnFlush(
	ctx context.Context,
	state FlushState,
) (service.MessageBatch, error) {
	now := time.Now().UnixMilli()
	if s.BatchSize <= 1 {
		return s.emitPerDevice(state, now)
	}
	return s.emitBatched(state, now)
}

func (s LogCompactedFlushStrategy) emitPerDevice(state FlushState, now int64) (service.MessageBatch, error) {
	var batch service.MessageBatch
	for vin, dev := range state {
		payload := model.Payload{
			NumOfData:  1,
			Data:       model.Data{ID: vin, Metrics: dev},
			ProducedAt: now,
		}
		msg := service.NewMessage(nil)
		msg.SetStructured(payload)
		msg.MetaSet("vincode", vin)
		batch = append(batch, msg)
	}
	return batch, nil
}

func (s LogCompactedFlushStrategy) emitBatched(state FlushState, now int64) (service.MessageBatch, error) {
	var batch service.MessageBatch
	var chunk []model.Data
	for vin, dev := range state {
		chunk = append(chunk, model.Data{ID: vin, Metrics: dev})
		if len(chunk) >= s.BatchSize {
			payload := model.PayloadBatch{
				NumOfData:  len(chunk),
				Data:       chunk,
				ProducedAt: now,
			}
			msg := service.NewMessage(nil)
			msg.SetStructured(payload)
			batch = append(batch, msg)
			chunk = nil
		}
	}
	if len(chunk) > 0 {
		payload := model.PayloadBatch{
			NumOfData:  len(chunk),
			Data:       chunk,
			ProducedAt: now,
		}
		msg := service.NewMessage(nil)
		msg.SetStructured(payload)
		batch = append(batch, msg)
	}
	return batch, nil
}
