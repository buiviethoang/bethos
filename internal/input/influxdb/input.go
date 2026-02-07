package influxdb

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"bethos/internal/model"
	"bethos/internal/resource"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/query"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	defaultChunkDuration = 2 * time.Second
	defaultLookback      = 10 * time.Second
	defaultBatchSize     = 5000
)

// InfluxDBInput implements service.BatchInput for querying InfluxDB by Pod in 2s chunks (10s lookback)
// and emitting telemetry records (id, vincode, resource_id, resource_name, value, captured_ts, ts, source).
type InfluxDBInput struct {
	url             string
	token           string
	org             string
	bucket          string
	pods            []string
	chunkDuration   time.Duration
	lookback        time.Duration
	resourceMapPath string
	batchSize       int
	measurement     string

	client   influxdb2.Client
	queryAPI api.QueryAPI
	cache    *resource.Cache
	// cursor: pod index, chunk index within lookback window
	podIndex   int
	chunkIndex int
	cycleStart time.Time
	done       bool
	pending    []*service.Message
}

// Config for the InfluxDB input (parsed from Bento config).
type Config struct {
	URL             string
	Token           string
	Org             string
	Bucket          string
	Pods            []string
	ChunkDuration   time.Duration
	Lookback        time.Duration
	ResourceMapPath string
	BatchSize       int
	Measurement     string
}

func New(cfg Config) (*InfluxDBInput, error) {
	if cfg.ChunkDuration <= 0 {
		cfg.ChunkDuration = defaultChunkDuration
	}
	if cfg.Lookback <= 0 {
		cfg.Lookback = defaultLookback
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.Measurement == "" {
		cfg.Measurement = "telemetry"
	}
	cache, err := resource.LoadFromResourceMatrix(cfg.ResourceMapPath)
	if err != nil {
		return nil, fmt.Errorf("influxdb input: load resource matrix: %w", err)
	}
	return &InfluxDBInput{
		url:             cfg.URL,
		token:           cfg.Token,
		org:             cfg.Org,
		bucket:          cfg.Bucket,
		pods:            cfg.Pods,
		chunkDuration:   cfg.ChunkDuration,
		lookback:        cfg.Lookback,
		resourceMapPath: cfg.ResourceMapPath,
		batchSize:       cfg.BatchSize,
		measurement:     cfg.Measurement,
		cache:           cache,
		podIndex:        0,
		chunkIndex:      0,
		cycleStart:      time.Now().Add(-cfg.Lookback),
		pending:         nil,
	}, nil
}

func (i *InfluxDBInput) Connect(ctx context.Context) error {
	if i.client != nil {
		return nil
	}
	i.client = influxdb2.NewClient(i.url, i.token)
	i.queryAPI = i.client.QueryAPI(i.org)
	return nil
}

func (i *InfluxDBInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if i.done {
		return nil, nil, service.ErrEndOfInput
	}

	// Drain pending first
	if len(i.pending) > 0 {
		n := i.batchSize
		if n > len(i.pending) {
			n = len(i.pending)
		}
		batch := i.pending[:n]
		i.pending = i.pending[n:]
		return batch, func(context.Context, error) error { return nil }, nil
	}

	// No pods configured: end input
	if len(i.pods) == 0 {
		i.done = true
		return nil, nil, service.ErrEndOfInput
	}

	chunksPerCycle := int(i.lookback / i.chunkDuration)
	if chunksPerCycle < 1 {
		chunksPerCycle = 1
	}
	if i.chunkIndex >= chunksPerCycle {
		i.chunkIndex = 0
		i.podIndex++
		if i.podIndex >= len(i.pods) {
			i.done = true
			return nil, nil, service.ErrEndOfInput
		}
		return i.ReadBatch(ctx)
	}

	start := i.cycleStart.Add(time.Duration(i.chunkIndex) * i.chunkDuration)
	end := start.Add(i.chunkDuration)
	pod := i.pods[i.podIndex]
	// Flux: from(bucket) |> range(start, stop) |> filter(measurement, pod)
	fluxQuery := fmt.Sprintf(`from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r["_measurement"] == "%s" and r["pod"] == "%s")`,
		i.bucket,
		start.UTC().Format(time.RFC3339Nano),
		end.UTC().Format(time.RFC3339Nano),
		i.measurement,
		pod,
	)

	result, err := i.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, nil, err
	}

	var batch service.MessageBatch
	for result.Next() {
		rec := result.Record()
		row := i.recordToRow(rec, pod)
		if row == nil {
			continue
		}
		msg := service.NewMessage(nil)
		msg.SetStructured(row)
		batch = append(batch, msg)
	}
	if result.Err() != nil {
		_ = result.Close()
		return nil, nil, result.Err()
	}
	_ = result.Close()

	i.chunkIndex++

	if len(batch) > i.batchSize {
		i.pending = batch[i.batchSize:]
		batch = batch[:i.batchSize]
	}

	if len(batch) == 0 {
		return i.ReadBatch(ctx)
	}

	return batch, func(context.Context, error) error { return nil }, nil
}

func (i *InfluxDBInput) recordToRow(rec *query.FluxRecord, pod string) *model.CSVRow {
	ts := rec.Time()
	value := rec.Value()
	if value == nil {
		return nil
	}
	vincode, _ := rec.ValueByKey("vincode").(string)
	if vincode == "" {
		vincode, _ = rec.ValueByKey("vin").(string)
	}
	resourceID, _ := rec.ValueByKey("resource_id").(string)
	if resourceID == "" {
		resourceID = rec.Field()
	}
	resourceName := resourceID
	if i.cache != nil {
		if n, ok := i.cache.IDToName[resourceID]; ok {
			resourceName = n
		}
	}
	valueStr := fmt.Sprintf("%v", value)
	ms := ts.UnixMilli()
	ns := ts.UnixNano()
	return &model.CSVRow{
		ID:           "influx_" + strconv.FormatInt(ns, 10),
		Vincode:      vincode,
		ResourceID:   resourceID,
		ResourceName: resourceName,
		Value:        valueStr,
		CapturedTS:   ms,
		TS:           ms,
		Source:       "influx",
		NsTS:         ns,
	}
}

func (i *InfluxDBInput) Close(ctx context.Context) error {
	if i.client != nil {
		i.client.Close()
		i.client = nil
		i.queryAPI = nil
	}
	return nil
}
