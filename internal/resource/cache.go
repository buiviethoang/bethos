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

// resourceMatrixFile is the JSON shape of config/resource_matrix.json.
type resourceMatrixFile struct {
	ResourceMatrix []Resource `json:"resource_matrix"`
}

// Load reads a JSON array of Resource (no wrapper).
func Load(path string) (*Cache, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var list []Resource
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	return buildCache(list), nil
}

// LoadFromResourceMatrix reads resource_matrix.json with wrapper {"resource_matrix": [...]}.
func LoadFromResourceMatrix(path string) (*Cache, error) {
	list, err := LoadResourceList(path)
	if err != nil {
		return nil, err
	}
	return buildCache(list), nil
}

// LoadResourceList reads resource_matrix.json and returns unique resources (by resource_id), first occurrence kept.
func LoadResourceList(path string) ([]Resource, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var wrapper resourceMatrixFile
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	out := make([]Resource, 0, len(wrapper.ResourceMatrix))
	for _, r := range wrapper.ResourceMatrix {
		if r.ResourceID == "" {
			continue
		}
		if _, ok := seen[r.ResourceID]; ok {
			continue
		}
		seen[r.ResourceID] = struct{}{}
		out = append(out, r)
	}
	return out, nil
}

func buildCache(list []Resource) *Cache {
	m := make(map[string]string, len(list))
	for _, r := range list {
		m[r.ResourceID] = r.ResourceName
	}
	return &Cache{IDToName: m}
}
