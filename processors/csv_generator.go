package processors

import (
	"bethos/internal/resource"
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

const defaultBulkBufferBytes = 32 * 1024 * 1024 // 32MB

// Default resources if no resource_map_path (id, name).
var defaultResources = []resource.Resource{
	{ResourceID: "content.34183.1.2", ResourceName: "vehicle_speed"},
	{ResourceID: "content.34183.1.3", ResourceName: "odometer"},
	{ResourceID: "content.10351.1.50", ResourceName: "door_status"},
	{ResourceID: "content.34183.1.4", ResourceName: "fuel_level"},
	{ResourceID: "content.34183.1.7", ResourceName: "ambient_temperature"},
	{ResourceID: "content.6.1.0", ResourceName: "latitude"},
	{ResourceID: "content.6.1.1", ResourceName: "longitude"},
}

type CSVGenerator struct {
	FilePath            string
	Count               int
	BulkBufferSize      int
	NumVincodes         int
	ResourceMapPath     string
	Seed                int64
	TruncateBeforeWrite bool

	resources []resource.Resource
	vincodes  []string
	rng       *rand.Rand
}

func (c *CSVGenerator) Close(ctx context.Context) error {
	return nil
}

func (c *CSVGenerator) init() error {
	if c.rng != nil {
		return nil
	}
	if c.NumVincodes <= 0 {
		c.NumVincodes = 1
	}
	// Build vincode list: VF37ARFZE + 8-digit index
	c.vincodes = make([]string, c.NumVincodes)
	for i := 0; i < c.NumVincodes; i++ {
		c.vincodes[i] = fmt.Sprintf("VF37ARFZE%08d", i+1)
	}
	// Load resources from matrix or use default
	if c.ResourceMapPath != "" {
		list, err := resource.LoadResourceList(c.ResourceMapPath)
		if err != nil {
			return err
		}
		if len(list) == 0 {
			c.resources = defaultResources
		} else {
			c.resources = list
		}
	} else {
		c.resources = defaultResources
	}
	seed := c.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	c.rng = rand.New(rand.NewSource(seed))
	return nil
}

// generalizedValue returns a plausible string value for the sensor (by resource_name).
func (c *CSVGenerator) generalizedValue(resourceName string) string {
	name := strings.ToLower(resourceName)
	switch {
	case strings.Contains(name, "speed") || strings.Contains(name, "velocity"):
		return strconv.Itoa(c.rng.Intn(121))
	case strings.Contains(name, "odometer") || strings.Contains(name, "distance"):
		return strconv.Itoa(c.rng.Intn(500000))
	case strings.Contains(name, "door") && strings.Contains(name, "status"):
		if c.rng.Intn(2) == 0 {
			return "Open"
		}
		return "Closed"
	case strings.Contains(name, "temperature") || strings.Contains(name, "pressure"):
		return strconv.Itoa(c.rng.Intn(100) + 10)
	case strings.Contains(name, "soc") || strings.Contains(name, "level") || strings.Contains(name, "fuel"):
		return strconv.Itoa(c.rng.Intn(101))
	case strings.Contains(name, "latitude"):
		return fmt.Sprintf("%.6f", 10+float64(c.rng.Intn(20)))
	case strings.Contains(name, "longitude"):
		return fmt.Sprintf("%.6f", 100+float64(c.rng.Intn(20)))
	case strings.Contains(name, "status") || strings.Contains(name, "mode"):
		statuses := []string{"Active", "Inactive", "Open", "Closed", "On", "Off"}
		return statuses[c.rng.Intn(len(statuses))]
	default:
		return strconv.Itoa(c.rng.Intn(1000))
	}
}

func (c *CSVGenerator) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	if err := c.init(); err != nil {
		return nil, err
	}

	flags := os.O_CREATE | os.O_WRONLY
	if c.TruncateBeforeWrite {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_APPEND
	}
	f, err := os.OpenFile(c.FilePath, flags, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	bufSize := c.BulkBufferSize
	if bufSize <= 0 {
		bufSize = defaultBulkBufferBytes
	}
	w := bufio.NewWriterSize(f, bufSize)
	defer w.Flush()

	start := time.Now()
	now := time.Now().UnixMilli()
	baseNs := time.Now().UnixNano()

	const writeChunk = 50000
	lineBuf := make([]string, 0, writeChunk)

	for i := 0; i < c.Count; i++ {
		vinIdx := c.rng.Intn(len(c.vincodes))
		resIdx := c.rng.Intn(len(c.resources))
		res := c.resources[resIdx]
		ts := now + int64(i)
		ns := baseNs + int64(i)
		value := c.generalizedValue(res.ResourceName)

		line := strings.Join([]string{
			fmt.Sprintf("tel_%d", i+1),
			c.vincodes[vinIdx],
			res.ResourceID,
			res.ResourceName,
			value,
			strconv.FormatInt(ts, 10),
			strconv.FormatInt(ts+int64(c.rng.Intn(1000)), 10),
			"bk",
			strconv.FormatInt(ns, 10),
		}, ",") + "\n"
		lineBuf = append(lineBuf, line)

		if len(lineBuf) >= writeChunk {
			for _, l := range lineBuf {
				if _, err := w.WriteString(l); err != nil {
					return nil, err
				}
			}
			lineBuf = lineBuf[:0]
		}
	}
	for _, l := range lineBuf {
		if _, err := w.WriteString(l); err != nil {
			return nil, err
		}
	}

	elapsed := time.Since(start)
	log.Printf("csv_generator: wrote %d records in %v (%d vincodes, %d sensors)", c.Count, elapsed, len(c.vincodes), len(c.resources))
	return service.MessageBatch{msg}, nil
}
