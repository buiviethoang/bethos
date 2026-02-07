package model

type Payload struct {
	NumOfData  int   `json:"num_of_data"`
	Data       Data  `json:"data"`
	ProducedAt int64 `json:"produced_at"`
}

type Data struct {
	ID      string                 `json:"id"`
	Metrics map[string]MetricValue `json:"-"`
}
