package processors

import (
	"bethos/internal/model"
	"bethos/internal/resource"
	"context"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type Aggregator struct {
	ResourceCache *resource.Cache
}

func (a *Aggregator) Close(ctx context.Context) error {
	return nil
}

func (a *Aggregator) ProcessBatch(
	ctx context.Context,
	batch service.MessageBatch,
) ([]service.MessageBatch, error) {

	state := make(map[string]map[string]model.MetricValue)
	sourceMsg := make(map[string]*service.Message)

	for _, msg := range batch {
		obj, err := msg.AsStructured()
		if err != nil {
			msg.SetError(err)
			continue
		}

		row, ok := obj.(model.CSVRow)
		if !ok {
			continue
		}

		resourceName, ok := a.ResourceCache.IDToName[row.ResourceID]
		if !ok {
			// unknown or inactive resource
			continue
		}

		// Keep one source message per VIN
		if _, exists := sourceMsg[row.Vincode]; !exists {
			sourceMsg[row.Vincode] = msg
		}

		v := state[row.Vincode]
		if v == nil {
			v = make(map[string]model.MetricValue)
			state[row.Vincode] = v
		}

		receivedAt := row.TS
		if receivedAt == 0 {
			receivedAt = time.Now().UnixMilli()
		}
		v[resourceName] = model.MetricValue{
			Value:      row.Value,
			ReceivedAt: receivedAt,
		}
	}

	// Build ONE output batch
	outBatch := service.MessageBatch{}
	now := time.Now().UnixMilli()

	for vin, metrics := range state {
		src := sourceMsg[vin]
		newMsg := src.Copy()
		data := map[string]any{
			"id": vin,
		}

		for name, metric := range metrics {
			data[name] = metric
		}

		payload := map[string]any{
			"num_of_data": 1,
			"data":        data,
			"produce_at":  now,
		}

		newMsg.SetStructured(payload)
		outBatch = append(outBatch, newMsg)
	}
	// Return slice of batches
	return []service.MessageBatch{outBatch}, nil
}
