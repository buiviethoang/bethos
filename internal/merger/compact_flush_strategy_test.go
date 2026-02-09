package merger

import (
	"context"
	"testing"
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
	payload, ok := obj.(map[string]any)
	if !ok {
		t.Fatalf("payload not map: %T", obj)
	}
	data, _ := payload["data"].(map[string]any)
	if data["id"] != "VIN1" {
		t.Errorf("data.id = %v, want VIN1", data["id"])
	}
	sensorA, _ := data["sensor_a"].(map[string]any)
	if sensorA["value"] != "1" {
		t.Errorf("data.sensor_a.value = %v, want 1", sensorA["value"])
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
	payload, _ := obj.(map[string]any)
	producedAt, ok := payload["produced_at"].(float64) // JSON number
	if !ok || producedAt <= 0 {
		t.Errorf("produced_at not set or invalid: %v", payload["produced_at"])
	}
}
