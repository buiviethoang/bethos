package merger

import (
	"context"
	"fmt"
	"testing"

	"bethos/internal/model"
)

func TestLogCompactedFlushStrategy_OnFlush_Empty(t *testing.T) {
	ctx := context.Background()
	s := LogCompactedFlushStrategy{}
	state := FlushState{}

	batch, err := s.OnFlush(ctx, state)
	if err != nil {
		t.Fatalf("OnFlush: %v", err)
	}
	if batch != nil && len(batch) != 0 {
		t.Errorf("expected nil or empty batch for empty state, got len=%d", len(batch))
	}
}

func TestLogCompactedFlushStrategy_OnFlush_OneVIN(t *testing.T) {
	ctx := context.Background()
	s := LogCompactedFlushStrategy{}
	state := FlushState{
		"VIN1": {
			"sensor_a": {Value: "1", ReceivedAt: 100},
		},
	}

	batch, err := s.OnFlush(ctx, state)
	if err != nil {
		t.Fatalf("OnFlush: %v", err)
	}
	if batch == nil || len(batch) != 1 {
		t.Fatalf("expected 1 message, got %v", batch)
	}

	vin, _ := batch[0].MetaGet("vincode")
	if vin != "VIN1" {
		t.Errorf("meta vincode = %q, want VIN1", vin)
	}

	obj, err := batch[0].AsStructured()
	if err != nil {
		t.Fatalf("AsStructured: %v", err)
	}
	if payload, ok := obj.(model.Payload); ok {
		if payload.Data.ID != "VIN1" {
			t.Errorf("data.id = %v, want VIN1", payload.Data.ID)
		}
		if v, ok := payload.Data.Metrics["sensor_a"]; ok && v.Value != "1" {
			t.Errorf("data.sensor_a.value = %v, want 1", v.Value)
		}
	} else if payload, ok := obj.(map[string]any); ok {
		data, _ := payload["data"].(map[string]any)
		if data["id"] != "VIN1" {
			t.Errorf("data.id = %v, want VIN1", data["id"])
		}
		sensorA, _ := data["sensor_a"].(map[string]any)
		if sensorA["value"] != "1" {
			t.Errorf("data.sensor_a.value = %v, want 1", sensorA["value"])
		}
	} else {
		t.Fatalf("payload type %T", obj)
	}
}

func TestLogCompactedFlushStrategy_OnFlush_TwoVINs(t *testing.T) {
	ctx := context.Background()
	s := LogCompactedFlushStrategy{}
	state := FlushState{
		"VIN_A": {"s1": {Value: "a", ReceivedAt: 1}},
		"VIN_B": {"s1": {Value: "b", ReceivedAt: 2}},
	}

	batch, err := s.OnFlush(ctx, state)
	if err != nil {
		t.Fatalf("OnFlush: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(batch))
	}

	vins := make(map[string]bool)
	for _, msg := range batch {
		vin, _ := msg.MetaGet("vincode")
		vins[vin] = true
	}
	if !vins["VIN_A"] || !vins["VIN_B"] {
		t.Errorf("expected both VIN_A and VIN_B in batch, got %v", vins)
	}
}

func TestLogCompactedFlushStrategy_OnFlush_ProducedAtSet(t *testing.T) {
	ctx := context.Background()
	s := LogCompactedFlushStrategy{}
	state := FlushState{"VIN1": {}}

	batch, err := s.OnFlush(ctx, state)
	if err != nil || len(batch) != 1 {
		t.Fatalf("OnFlush: err=%v len=%d", err, len(batch))
	}
	obj, _ := batch[0].AsStructured()
	switch p := obj.(type) {
	case model.Payload:
		if p.ProducedAt <= 0 {
			t.Errorf("produced_at not set: %d", p.ProducedAt)
		}
	case map[string]any:
		producedAt, ok := p["produced_at"].(float64)
		if !ok || producedAt <= 0 {
			t.Errorf("produced_at not set or invalid: %v", p["produced_at"])
		}
	default:
		t.Errorf("unexpected type %T", obj)
	}
}

func TestLogCompactedFlushStrategy_OnFlush_Batched(t *testing.T) {
	ctx := context.Background()
	s := LogCompactedFlushStrategy{BatchSize: 100}
	state := make(FlushState, 250)
	for i := 0; i < 250; i++ {
		state[fmt.Sprintf("VIN%d", i)] = map[string]model.MetricValue{"s": {Value: i, ReceivedAt: int64(i)}}
	}

	batch, err := s.OnFlush(ctx, state)
	if err != nil {
		t.Fatalf("OnFlush: %v", err)
	}
	// 250 devices / 100 per message = 3 messages
	if len(batch) != 3 {
		t.Fatalf("expected 3 batched messages, got %d", len(batch))
	}
	// First two messages have 100 devices each, last has 50
	obj, _ := batch[0].AsStructured()
	pb, ok := obj.(model.PayloadBatch)
	if !ok {
		t.Fatalf("expected PayloadBatch, got %T", obj)
	}
	if pb.NumOfData != 100 {
		t.Errorf("first message num_of_data = %d, want 100", pb.NumOfData)
	}
	obj2, _ := batch[2].AsStructured()
	pb2, ok := obj2.(model.PayloadBatch)
	if !ok {
		t.Fatalf("expected PayloadBatch, got %T", obj2)
	}
	if pb2.NumOfData != 50 {
		t.Errorf("last message num_of_data = %d, want 50", pb2.NumOfData)
	}
}
