package model

import "encoding/json"

type Payload struct {
	NumOfData  int   `json:"num_of_data"`
	Data       Data  `json:"data"`
	ProducedAt int64 `json:"produced_at"`
}

// PayloadBatch is a single message containing multiple devices (for batched Kafka output to reduce network I/O).
type PayloadBatch struct {
	NumOfData  int    `json:"num_of_data"`
	Data       []Data `json:"data"`
	ProducedAt int64  `json:"produced_at"`
}

type Data struct {
	ID      string                 `json:"id"`
	Metrics map[string]MetricValue `json:"-"`
}

func (d *Data) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	d.Metrics = make(map[string]MetricValue)

	for k, v := range raw {
		if k == "id" {
			if err := json.Unmarshal(v, &d.ID); err != nil {
				return err
			}
			continue
		}

		var mv MetricValue
		if err := json.Unmarshal(v, &mv); err != nil {
			return err
		}

		d.Metrics[k] = mv
	}

	return nil
}

func (d Data) MarshalJSON() ([]byte, error) {
	out := make(map[string]any, len(d.Metrics)+1)

	if d.ID != "" {
		out["id"] = d.ID
	}

	for k, v := range d.Metrics {
		out[k] = v
	}

	return json.Marshal(out)
}
