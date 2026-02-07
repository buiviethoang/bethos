package model

type MetricValue struct {
	Value      any   `json:"value"`
	ReceivedAt int64 `json:"received_at"`
}
