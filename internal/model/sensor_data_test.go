package model

import (
	"encoding/json"
	"testing"
)

func TestData_UnmarshalJSON_WithIDAndSensors(t *testing.T) {
	raw := `{"id":"VIN1","sensor_a":{"value":"541","received_at":1770629822367}}`
	var d Data
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if d.ID != "VIN1" {
		t.Errorf("ID = %q, want VIN1", d.ID)
	}
	if len(d.Metrics) != 1 {
		t.Fatalf("len(Metrics) = %d, want 1", len(d.Metrics))
	}
	mv := d.Metrics["sensor_a"]
	if mv.ReceivedAt != 1770629822367 {
		t.Errorf("ReceivedAt = %d, want 1770629822367", mv.ReceivedAt)
	}
	if v, ok := mv.Value.(string); !ok || v != "541" {
		t.Errorf("Value = %v, want \"541\"", mv.Value)
	}
}

func TestData_UnmarshalJSON_IDLast(t *testing.T) {
	// id can appear after other keys (map iteration order in JSON)
	raw := `{"sensor_a":{"value":"x","received_at":123},"id":"VIN1"}`
	var d Data
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if d.ID != "VIN1" {
		t.Errorf("ID = %q, want VIN1", d.ID)
	}
	if d.Metrics["sensor_a"].ReceivedAt != 123 {
		t.Errorf("Metrics[sensor_a].ReceivedAt = %d", d.Metrics["sensor_a"].ReceivedAt)
	}
}

func TestData_UnmarshalJSON_OnlyID(t *testing.T) {
	raw := `{"id":"VIN1"}`
	var d Data
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if d.ID != "VIN1" {
		t.Errorf("ID = %q, want VIN1", d.ID)
	}
	if len(d.Metrics) != 0 {
		t.Errorf("len(Metrics) = %d, want 0", len(d.Metrics))
	}
}

func TestPayload_Unmarshal_Full(t *testing.T) {
	raw := `{"num_of_data":1,"data":{"id":"VF37ARFZE00000171","accelerator_pedal_position":{"value":"541","received_at":1770629822367}},"produced_at":1770632664525}`
	var p Payload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if p.NumOfData != 1 {
		t.Errorf("NumOfData = %d", p.NumOfData)
	}
	if p.Data.ID != "VF37ARFZE00000171" {
		t.Errorf("Data.ID = %q", p.Data.ID)
	}
	if p.ProducedAt != 1770632664525 {
		t.Errorf("ProducedAt = %d", p.ProducedAt)
	}
	if p.Data.Metrics["accelerator_pedal_position"].ReceivedAt != 1770629822367 {
		t.Errorf("Metrics[accelerator_pedal_position].ReceivedAt wrong")
	}
}

func TestData_MarshalJSON_Roundtrip(t *testing.T) {
	d := Data{
		ID: "VIN1",
		Metrics: map[string]MetricValue{
			"sensor_a": {Value: "541", ReceivedAt: 1770629822367},
		},
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Data
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != d.ID {
		t.Errorf("roundtrip ID = %q", decoded.ID)
	}
	if len(decoded.Metrics) != 1 || decoded.Metrics["sensor_a"].ReceivedAt != d.Metrics["sensor_a"].ReceivedAt {
		t.Errorf("roundtrip Metrics mismatch")
	}
}
