package influxdb

import (
	"context"
	"fmt"
	"math/rand"
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
	defaultChunkDuration     = 2 * time.Second
	defaultLookback          = 10 * time.Second
	defaultBatchSize         = 5000
	defaultRetryMaxAttempts  = 3
	defaultRetryInitial      = 1 * time.Second
	defaultRetryMaxInterval  = 30 * time.Second
	defaultRetryMultiplier   = 2.0
	defaultRetryJitterFactor = 0.1
)

// InfluxDBInput implements service.BatchInput for querying InfluxDB by Pod in chunks (lookback window).
// When tick_interval > 0, it runs indefinitely: each cycle uses lookback from now, then waits tick_interval before the next cycle (same as ETL flow ticking every 10s).
type InfluxDBInput struct {
	url             string
	token           string
	org             string
	bucket          string
	pods            []string
	chunkDuration   time.Duration
	lookback        time.Duration
	tickInterval    time.Duration // if > 0, wait this long between cycles and loop; if 0, run one cycle then end
	resourceMapPath string
	batchSize       int
	measurement     string
	// retry for fault tolerance
	retryMaxAttempts  int
	retryInitial      time.Duration
	retryMaxInterval  time.Duration
	retryMultiplier   float64
	retryJitterFactor float64
	checkpointPath    string // optional: persist cycle end for idempotency (Option A)

	client   influxdb2.Client
	queryAPI api.QueryAPI
	cache    *resource.Cache
	// cursor: pod index, chunk index within lookback window
	podIndex     int
	chunkIndex   int
	cycleStart   time.Time
	done         bool
	pending      []*service.Message
	betweenCycles bool   // true when waiting for next tick before starting a new cycle
	nextCycleAt  time.Time
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
	TickInterval    time.Duration // 0 = one cycle then end; >0 = wait between cycles and loop (e.g. 10s)
	ResourceMapPath string
	BatchSize       int
	Measurement     string
	// Retry for InfluxDB query failures (fault tolerance)
	RetryMaxAttempts  int
	RetryInitial      time.Duration
	RetryMaxInterval  time.Duration
	RetryMultiplier   float64
	RetryJitterFactor float64
	CheckpointPath    string // optional: persist cycle end time to avoid reprocessing on restart
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
	if cfg.RetryMaxAttempts <= 0 {
		cfg.RetryMaxAttempts = defaultRetryMaxAttempts
	}
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = defaultRetryInitial
	}
	if cfg.RetryMaxInterval <= 0 {
		cfg.RetryMaxInterval = defaultRetryMaxInterval
	}
	if cfg.RetryMultiplier <= 0 {
		cfg.RetryMultiplier = defaultRetryMultiplier
	}
	if cfg.RetryJitterFactor <= 0 {
		cfg.RetryJitterFactor = defaultRetryJitterFactor
	}
	cache, err := resource.LoadFromResourceMatrix(cfg.ResourceMapPath)
	if err != nil {
		return nil, fmt.Errorf("influxdb input: load resource matrix: %w", err)
	}
	return &InfluxDBInput{
		url:               cfg.URL,
		token:             cfg.Token,
		org:               cfg.Org,
		bucket:            cfg.Bucket,
		pods:              cfg.Pods,
		chunkDuration:     cfg.ChunkDuration,
		lookback:          cfg.Lookback,
		tickInterval:      cfg.TickInterval,
		resourceMapPath:   cfg.ResourceMapPath,
		batchSize:         cfg.BatchSize,
		measurement:       cfg.Measurement,
		retryMaxAttempts:  cfg.RetryMaxAttempts,
		retryInitial:     cfg.RetryInitial,
		retryMaxInterval:  cfg.RetryMaxInterval,
		retryMultiplier:   cfg.RetryMultiplier,
		retryJitterFactor: cfg.RetryJitterFactor,
		checkpointPath:    cfg.CheckpointPath,
		cache:             cache,
		podIndex:          0,
		chunkIndex:        0,
		cycleStart:        time.Now().Add(-cfg.Lookback),
		pending:           nil,
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

	// Between cycles: wait for next tick then start a new cycle (same as ETL ticking every 10s)
	if i.betweenCycles {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(time.Until(i.nextCycleAt)):
			// fall through
		}
		now := time.Now()
		i.cycleStart = now.Add(-i.lookback)
		if i.checkpointPath != "" {
			if cp, err := LoadCheckpoint(i.checkpointPath); err == nil && !cp.IsZero() {
				if cp.After(i.cycleStart) {
					i.cycleStart = cp
				}
			}
		}
		i.podIndex = 0
		i.chunkIndex = 0
		i.betweenCycles = false
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
			// Cycle complete: optional checkpoint (idempotency Option A)
			if i.checkpointPath != "" {
				cycleEnd := i.cycleStart.Add(i.lookback)
				_ = SaveCheckpoint(i.checkpointPath, cycleEnd)
			}
			if i.tickInterval > 0 {
				i.betweenCycles = true
				i.nextCycleAt = time.Now().Add(i.tickInterval)
				return i.ReadBatch(ctx)
			}
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

	var batch service.MessageBatch
	var lastErr error
	for attempt := 0; attempt < i.retryMaxAttempts; attempt++ {
		if attempt > 0 {
			backoff := i.retryInitial
			for k := 0; k < attempt && backoff < i.retryMaxInterval; k++ {
				backoff = time.Duration(float64(backoff) * i.retryMultiplier)
			}
			if backoff > i.retryMaxInterval {
				backoff = i.retryMaxInterval
			}
			jitter := time.Duration(float64(backoff) * i.retryJitterFactor * (2*rand.Float64() - 1))
			backoff += jitter
			if backoff < 0 {
				backoff = i.retryInitial
			}
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		result, err := i.queryAPI.Query(ctx, fluxQuery)
		if err != nil {
			lastErr = err
			continue
		}
		batch = batch[:0]
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
		queryErr := result.Err()
		_ = result.Close()
		if queryErr != nil {
			lastErr = queryErr
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		return nil, nil, lastErr
	}

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
