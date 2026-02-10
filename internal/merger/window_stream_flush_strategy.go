package merger

import (
	"bethos/internal/model"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type WindowStreamStrategy struct {
	WindowSize      time.Duration // e.g. 2m
	AllowedLateness time.Duration // e.g. 10s
	EmitOnClose     bool

	mu      sync.Mutex
	windows map[int64]map[string]model.MetricValue // windowStart -> vin -> metrics
}

func NewWindowStreamStrategy() *WindowStreamStrategy {
	return &WindowStreamStrategy{
		WindowSize:      2 * time.Minute,
		AllowedLateness: 10 * time.Second,
		EmitOnClose:     true,
		windows:         make(map[int64]map[string]model.MetricValue),
	}
}

func (w *WindowStreamStrategy) Close(ctx context.Context) error {
	return nil
}

func (w *WindowStreamStrategy) windowStart(ts int64) int64 {
	return ts - (ts % w.WindowSize.Milliseconds())
}

func (w *WindowStreamStrategy) OnFlush(
	ctx context.Context,
	state FlushState,
) (service.MessageBatch, error) {
	now := time.Now().UnixMilli()
	currentWindow := w.windowStart(now)

	w.mu.Lock()
	defer w.mu.Unlock()

	// merge snapshot into current window
	win := w.windows[currentWindow]
	if win == nil {
		win = make(map[string]model.MetricValue)
		w.windows[currentWindow] = win
	}
	for vin, metrics := range state {
		win[vin] = model.MetricValue{
			Value:      metrics,
			ReceivedAt: now,
		}
	}

	var batch service.MessageBatch
	for ws, data := range w.windows {
		if ws+w.WindowSize.Milliseconds()+w.AllowedLateness.Milliseconds() <= now {
			for vin, metric := range data {
				payload := model.Payload{
					NumOfData:  1,
					Data:      model.Data{ID: vin, Metrics: metric.Value.(map[string]model.MetricValue)},
					ProducedAt: ws + int64(w.WindowSize.Milliseconds()),
				}
				msg := service.NewMessage(nil)
				msg.SetStructured(payload)
				msg.MetaSet("kafka_key", vin)
				msg.MetaSet("window_start", fmt.Sprintf("%d", ws))
				batch = append(batch, msg)
			}
			delete(w.windows, ws)
		}
	}
	return batch, nil
}
