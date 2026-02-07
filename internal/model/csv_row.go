package model

type CSVRow struct {
	ID           string `json:"id"`
	Vincode      string `json:"vincode"`
	ResourceID   string `json:"resource_id"`
	ResourceName string `json:"resource_name"`
	Value        string `json:"value"`
	CapturedTS   int64  `json:"captured_ts"`
	TS           int64  `json:"ts"`
	Source       string `json:"source"`
	NsTS         int64  `json:"ns_ts"`
}
