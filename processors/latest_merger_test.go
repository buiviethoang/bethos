package processors

import (
	"bethos/internal/model"
	"context"
	"encoding/json"
	"testing"

	"bethos/internal/merger"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestIsFlushTrigger(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want bool
	}{
		{"no space", []byte(`{"_flush":true}`), true},
		{"with space", []byte(`{"_flush": true}`), true},
		{"false", []byte(`{"_flush":false}`), false},
		{"missing key", []byte(`{}`), false},
		{"not json", []byte(`not json`), false},
		{"other key", []byte(`{"x":1}`), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFlushTrigger(tt.raw); got != tt.want {
				t.Errorf("isFlushTrigger() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLatestMerger_Process_FlushTrigger(t *testing.T) {
	ctx := context.Background()
	strat := &merger.LogCompactedFlushStrategy{}
	m := &LatestMerger{Strategy: strat}

	msg := service.NewMessage([]byte(`{"_flush": true}`))
	batch, err := m.Process(ctx, msg)
	if err != nil {
		t.Fatalf("Process(flush trigger) err = %v", err)
	}
	// Empty state -> batch may be nil or empty
	if batch == nil {
		return
	}
	// If we had merged some VINs before, batch would have messages
	_ = batch
}

func TestLatestMerger_Process_EmptyVIN(t *testing.T) {
	ctx := context.Background()
	m := &LatestMerger{Strategy: &merger.LogCompactedFlushStrategy{}}

	// Payload without data.id (e.g. data has only sensor keys)
	body := `{"num_of_data":1,"data":{"sensor_a":{"value":"1","received_at":100}},"produced_at":1}`
	msg := service.NewMessage(nil)
	msg.SetBytes([]byte(body))

	batch, err := m.Process(ctx, msg)
	if err != nil {
		t.Fatalf("Process(empty vin) err = %v", err)
	}
	if batch != nil {
		t.Errorf("Process(empty vin) batch = %v, want nil", batch)
	}
}

func TestLatestMerger_Process_ValidPayload(t *testing.T) {
	ctx := context.Background()
	strat := &merger.LogCompactedFlushStrategy{}
	m := &LatestMerger{Strategy: strat}

	body := `{"num_of_data":1,"data":{"id":"VIN1","sensor_a":{"value":"1","received_at":100}},"produced_at":1}`
	msg := service.NewMessage(nil)
	msg.SetBytes([]byte(body))

	batch, err := m.Process(ctx, msg)
	if err != nil {
		t.Fatalf("Process(valid payload) err = %v", err)
	}
	if batch != nil {
		t.Errorf("Process(valid payload) expected nil batch (no flush), got len=%d", len(batch))
	}
}

func TestLatestMerger_Process_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	m := &LatestMerger{Strategy: &merger.LogCompactedFlushStrategy{}}

	msg := service.NewMessage(nil)
	msg.SetBytes([]byte(`not json`))

	batch, err := m.Process(ctx, msg)
	if err == nil {
		t.Error("Process(invalid json) expected error")
	}
	if batch != nil {
		t.Errorf("Process(invalid json) batch = %v, want nil", batch)
	}
}

func TestLatestMerger_Merge_LatestWins(t *testing.T) {
	ctx := context.Background()
	strat := &merger.LogCompactedFlushStrategy{}
	m := &LatestMerger{Strategy: strat}

	// First message: sensor_a at 100
	body1 := `{"num_of_data":1,"data":{"id":"VIN1","sensor_a":{"value":"old","received_at":100}},"produced_at":1}`
	msg1 := service.NewMessage(nil)
	msg1.SetBytes([]byte(body1))
	_, _ = m.Process(ctx, msg1)

	// Second message: sensor_a at 200 (newer)
	body2 := `{"num_of_data":1,"data":{"id":"VIN1","sensor_a":{"value":"new","received_at":200}},"produced_at":2}`
	msg2 := service.NewMessage(nil)
	msg2.SetBytes([]byte(body2))
	_, _ = m.Process(ctx, msg2)

	// Flush
	flushMsg := service.NewMessage([]byte(`{"_flush":true}`))
	batch, err := m.Process(ctx, flushMsg)
	if err != nil {
		t.Fatalf("Process(flush) err = %v", err)
	}
	if batch == nil || len(batch) != 1 {
		t.Fatalf("expected 1 message in batch, got %v", batch)
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
	sensorA, _ := data["sensor_a"].(map[string]any)
	value, _ := sensorA["value"].(string)
	if value != "new" {
		t.Errorf("sensor_a.value = %q, want \"new\" (latest received_at wins)", value)
	}
}

func TestLatestMerger_Merge_NewerReceivedAtWins(t *testing.T) {
	m := &LatestMerger{state: make(map[string]*DeviceState)}

	// Merge older first
	m.merge(model.Payload{
		Data: model.Data{
			ID: "VIN1",
			Metrics: map[string]model.MetricValue{
				"sensor_x": {Value: "first", ReceivedAt: 100},
			},
		},
	})
	// Merge newer
	m.merge(model.Payload{
		Data: model.Data{
			ID: "VIN1",
			Metrics: map[string]model.MetricValue{
				"sensor_x": {Value: "second", ReceivedAt: 200},
			},
		},
	})

	m.mu.Lock()
	dev := m.state["VIN1"]
	m.mu.Unlock()
	if dev == nil {
		t.Fatal("state[VIN1] is nil")
	}
	mv := dev.Metrics["sensor_x"]
	val, _ := mv.Value.(string)
	if val != "second" {
		t.Errorf("Metrics[sensor_x].Value = %q, want \"second\"", val)
	}
	if mv.ReceivedAt != 200 {
		t.Errorf("Metrics[sensor_x].ReceivedAt = %d, want 200", mv.ReceivedAt)
	}
}

func TestLatestMerger_Merge_OlderDoesNotOverwrite(t *testing.T) {
	m := &LatestMerger{state: make(map[string]*DeviceState)}

	m.merge(model.Payload{
		Data: model.Data{
			ID: "VIN1",
			Metrics: map[string]model.MetricValue{
				"sensor_x": {Value: "newer", ReceivedAt: 200},
			},
		},
	})
	m.merge(model.Payload{
		Data: model.Data{
			ID: "VIN1",
			Metrics: map[string]model.MetricValue{
				"sensor_x": {Value: "older", ReceivedAt: 100},
			},
		},
	})

	m.mu.Lock()
	dev := m.state["VIN1"]
	m.mu.Unlock()
	val, _ := dev.Metrics["sensor_x"].Value.(string)
	if val != "newer" {
		t.Errorf("older message overwrote newer: Value = %q, want \"newer\"", val)
	}
}

func TestLatestMerger_Flush_EmptyState(t *testing.T) {
	ctx := context.Background()
	strat := &merger.LogCompactedFlushStrategy{}
	m := &LatestMerger{Strategy: strat}

	msg := service.NewMessage([]byte(`{"_flush":true}`))
	batch, err := m.Process(ctx, msg)
	if err != nil {
		t.Fatalf("flush empty state: %v", err)
	}
	if len(batch) > 0 {
		t.Errorf("expected nil or empty batch for empty state, got len=%d", len(batch))
	}
}

func TestLatestMerger_Flush_TwoVINs(t *testing.T) {
	ctx := context.Background()
	strat := &merger.LogCompactedFlushStrategy{}
	m := &LatestMerger{Strategy: strat}

	for _, body := range []string{
		`{"num_of_data":1,"data":{"id":"VIN_A","s1":{"value":"a","received_at":1}},"produced_at":1}`,
		`{"num_of_data":1,"data":{"id":"VIN_B","s1":{"value":"b","received_at":2}},"produced_at":2}`,
	} {
		msg := service.NewMessage(nil)
		msg.SetBytes([]byte(body))
		_, _ = m.Process(ctx, msg)
	}

	flushMsg := service.NewMessage([]byte(`{"_flush":true}`))
	batch, err := m.Process(ctx, flushMsg)
	if err != nil {
		t.Fatalf("flush: %v", err)
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
		t.Errorf("expected vincode meta for VIN_A and VIN_B, got %v", vins)
	}
}

func TestLatestMerger_Merge_EmptyVINIgnored(t *testing.T) {
	m := &LatestMerger{state: make(map[string]*DeviceState)}

	m.merge(model.Payload{Data: model.Data{ID: ""}})
	m.merge(model.Payload{Data: model.Data{ID: "VIN1", Metrics: map[string]model.MetricValue{"s": {ReceivedAt: 1}}}})

	m.mu.Lock()
	n := len(m.state)
	m.mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 VIN in state (empty id ignored), got %d", n)
	}
}

// Test that payload with id and metrics unmarshals and merges (sanity)
func TestPayloadRoundtrip(t *testing.T) {
	payload := model.Payload{
		NumOfData: 1,
		Data: model.Data{
			ID: "VIN1",
			Metrics: map[string]model.MetricValue{
				"sensor_a": {Value: "541", ReceivedAt: 1770629822367},
			},
		},
		ProducedAt: 1770632664525,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var decoded model.Payload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Data.ID != payload.Data.ID {
		t.Errorf("id = %q, want %q", decoded.Data.ID, payload.Data.ID)
	}
	if len(decoded.Data.Metrics) != 1 {
		t.Errorf("len(Metrics) = %d, want 1", len(decoded.Data.Metrics))
	}
}
