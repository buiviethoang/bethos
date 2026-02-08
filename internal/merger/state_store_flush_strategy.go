package merger

import (
	"bethos/internal/model"
	"context"
	"encoding/json"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type StateStoreFlushStrategy struct {
	CacheName string
	Resources *service.Resources
}

func (s *StateStoreFlushStrategy) Close(ctx context.Context) error {
	return nil
}

func (s *StateStoreFlushStrategy) OnFlush(
	ctx context.Context,
	state FlushState,
) (service.MessageBatch, error) {

	if s.Resources == nil || s.CacheName == "" {
		return nil, nil
	}

	return nil, s.Resources.AccessCache(ctx, s.CacheName, func(c service.Cache) {
		ttl := 5 * time.Minute

		for vin, dev := range state {
			payload := model.Payload{
				NumOfData: 1,
				Data: model.Data{
					ID:      vin,
					Metrics: dev,
				},
				ProducedAt: time.Now().UnixMilli(),
			}

			b, _ := json.Marshal(payload)

			_ = c.Set(ctx, "latest:"+vin, b, &ttl)
		}
	})
}
