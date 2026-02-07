package processors

import (
	"context"
	"encoding/json"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	defaultIndexKey = "latest:__index__"
	defaultPrefix   = "latest:"
)

// LatestStatePublisher reads the merged latest state from cache (written by LatestMerger with StateStore strategy)
// and emits one message per device. Use with input generate(interval: 2m) and output Kafka destination.
type LatestStatePublisher struct {
	CacheName string
	IndexKey  string
	Prefix    string
	Resources *service.Resources
}

func (p *LatestStatePublisher) Close(ctx context.Context) error {
	return nil
}

func (p *LatestStatePublisher) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	if p.Resources == nil || p.CacheName == "" {
		return nil, nil
	}
	indexKey := p.IndexKey
	if indexKey == "" {
		indexKey = defaultIndexKey
	}
	prefix := p.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}

	var out service.MessageBatch
	var getErr error
	err := p.Resources.AccessCache(ctx, p.CacheName, func(c service.Cache) {
		indexBytes, err := c.Get(ctx, indexKey)
		if err != nil {
			getErr = err
			return
		}
		var vincodes []string
		if err := json.Unmarshal(indexBytes, &vincodes); err != nil {
			getErr = err
			return
		}
		for _, vin := range vincodes {
			payloadBytes, err := c.Get(ctx, prefix+vin)
			if err != nil {
				continue
			}
			var payload map[string]any
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				continue
			}
			newMsg := service.NewMessage(nil)
			newMsg.SetStructured(payload)
			newMsg.MetaSet("vincode", vin)
			out = append(out, newMsg)
		}
	})
	if err != nil {
		return nil, err
	}
	if getErr != nil {
		return nil, getErr
	}
	return out, nil
}
