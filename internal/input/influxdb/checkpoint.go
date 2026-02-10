package influxdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// LoadCheckpoint reads the last successfully processed end time (Unix ms) from path.
// If the file does not exist or is invalid, returns zero time and nil error (caller should use now - lookback).
func LoadCheckpoint(path string) (time.Time, error) {
	if path == "" {
		return time.Time{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	ms, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("checkpoint invalid: %w", err)
	}
	return time.Unix(0, ms*int64(time.Millisecond)), nil
}

// SaveCheckpoint writes the end time (Unix ms) of the last completed cycle to path.
// Creates parent directories if needed. File is written atomically (write to temp then rename).
func SaveCheckpoint(path string, endTime time.Time) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	ms := endTime.UnixMilli()
	data := strconv.FormatInt(ms, 10) + "\n"
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(data), 0640); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
