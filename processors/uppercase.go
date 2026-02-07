package processors

import (
	"context"
	"strings"

	"github.com/warpstreamlabs/bento/public/service"
)

type UppercaseProcessor struct{}

func (p *UppercaseProcessor) Process(
	ctx context.Context,
	msg *service.Message,
) (service.MessageBatch, error) {

	obj, err := msg.AsStructured()
	if err != nil {
		return nil, err
	}

	m := obj.(map[string]any)
	for k, v := range m {
		if s, ok := v.(string); ok {
			m[k] = strings.ToUpper(s)
		}
	}

	msg.SetStructured(m)
	return service.MessageBatch{msg}, nil
}

func (p *UppercaseProcessor) Close(ctx context.Context) error {
	return nil
}
