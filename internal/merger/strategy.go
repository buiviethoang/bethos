package merger

import (
	"bethos/internal/model"
	"context"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	StrategyInline       = "inline"
	StrategyStateStore   = "state_store"
	StrategyLogCompacted = "log_compacted"
	StrategyWindowStream = "window_stream"
)

type FlushState map[string]map[string]model.MetricValue

type FlushStrategy interface {
	OnFlush(ctx context.Context, state FlushState) (service.MessageBatch, error)
	Close(ctx context.Context) error
}
