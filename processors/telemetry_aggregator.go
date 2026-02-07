package processors

import (
	"bethos/internal/model"
	"context"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type Aggregator struct{}

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
		v[row.ResourceName] = model.MetricValue{
			Value:      row.Value,
			ReceivedAt: receivedAt,
		}
	}

	// Build ONE output batch
	outBatch := service.MessageBatch{}

	for vin, metrics := range state {
		src := sourceMsg[vin]

		// MUST derive from existing message
		newMsg := src.Copy()
		newMsg.SetStructured(map[string]any{
			"vincode": vin,
			"metrics": metrics,
		})

		outBatch = append(outBatch, newMsg)
	}
	// Return slice of batches
	return []service.MessageBatch{outBatch}, nil
}
