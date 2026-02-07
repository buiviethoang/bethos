package resource

import (
	"encoding/json"
	"os"
)

type Resource struct {
	ResourceID   string `json:"resource_id"`
	ResourceName string `json:"resource_name"`
}

type Cache struct {
	IDToName map[string]string
}

func Load(path string) (*Cache, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var list []Resource
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}

	m := make(map[string]string, len(list))
	for _, r := range list {
		m[r.ResourceID] = r.ResourceName
	}
	return &Cache{IDToName: m}, nil
}
