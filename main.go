package main

import (
	"bethos/internal/input/influxdb"
	"bethos/internal/merger"
	"bethos/internal/resource"
	"bethos/processors"
	"context"
	"log"
	"os"
	"time"

	_ "github.com/warpstreamlabs/bento/public/components/all"
	"github.com/warpstreamlabs/bento/public/service"
)

func main() {

	service.RegisterProcessor(
		"csv_generator",
		service.NewConfigSpec().
			Field(service.NewStringField("file_path")).
			Field(service.NewIntField("records_per_tick")).
			Field(service.NewIntField("bulk_buffer_bytes").Default(0)).
			Field(service.NewIntField("num_vincodes").Description("Number of distinct devices (vincodes) to spread rows across").Default(1)).
			Field(service.NewStringField("resource_map_path").Description("Path to resource_matrix.json for sensor list; empty = built-in list").Default("")).
			Field(service.NewIntField("seed").Description("Random seed for reproducible data; 0 = time-based").Default(0)).
			Field(service.NewBoolField("truncate_before_write").Description("If true, truncate the CSV file before each write (each tick gets a fresh file; use for varied ETL).").Default(false)),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {

			filePath, err := conf.FieldString("file_path")
			if err != nil {
				return nil, err
			}

			count, err := conf.FieldInt("records_per_tick")
			if err != nil {
				return nil, err
			}

			bufBytes, _ := conf.FieldInt("bulk_buffer_bytes")
			numVincodes, _ := conf.FieldInt("num_vincodes")
			resourceMapPath, _ := conf.FieldString("resource_map_path")
			seed, _ := conf.FieldInt("seed")
			truncate, _ := conf.FieldBool("truncate_before_write")

			return &processors.CSVGenerator{
				FilePath:            filePath,
				Count:               count,
				BulkBufferSize:      bufBytes,
				NumVincodes:         numVincodes,
				ResourceMapPath:     resourceMapPath,
				Seed:                int64(seed),
				TruncateBeforeWrite: truncate,
			}, nil
		},
	)

	service.RegisterProcessor(
		"csv_reader",
		service.NewConfigSpec().
			Field(service.NewStringField("file_path")).
			Field(service.NewIntField("batch_size")).
			Field(service.NewIntField("max_lines").Default(0)),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {

			filePath, err := conf.FieldString("file_path")
			if err != nil {
				return nil, err
			}

			batchSize, err := conf.FieldInt("batch_size")
			if err != nil {
				return nil, err
			}

			maxLines, _ := conf.FieldInt("max_lines")

			return &processors.CSVReader{
				FilePath: filePath,
				Batch:    batchSize,
				MaxLines: maxLines,
			}, nil
		},
	)

	service.RegisterBatchProcessor(
		"telemetry_aggregator",
		service.NewConfigSpec().
			Field(service.NewStringField("resource_matrix_path")),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {

			path, err := conf.FieldString("resource_matrix_path")
			if err != nil {
				return nil, err
			}

			cache, err := resource.LoadFromResourceMatrix(path)
			if err != nil {
				return nil, err
			}

			return &processors.Aggregator{
				ResourceCache: cache,
			}, nil
		},
	)

	service.RegisterProcessor(
		"kafka_message_builder",
		service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.Processor, error) {
			return &processors.KafkaBuilder{}, nil
		},
	)

	service.RegisterProcessor(
		"latest_merger",
		service.NewConfigSpec().
			Field(service.NewStringField("strategy").Description("inline = emit batch to output; state_store = write to cache, separate publisher reads and emits").Default("inline")).
			Field(service.NewStringField("cache").Description("Cache resource name for state_store strategy").Default("")).
			Field(service.NewStringField("cache_index_key").Description("Cache key for list of vincodes (state_store)").Default("")).
			Field(service.NewStringField("cache_prefix").Description("Cache key prefix per device (state_store)").Default("")).
			Field(service.NewIntField("batch_size").Description("For log_compacted: devices per message (1 = one message per device; >1 = batched to reduce network I/O). Default 1").Default(1)),
		func(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			strategyName, _ := conf.FieldString("strategy")
			cacheName, _ := conf.FieldString("cache")
			batchSize, _ := conf.FieldInt("batch_size")

			var strat merger.FlushStrategy
			switch strategyName {
			case merger.StrategyStateStore:
				strat = &merger.StateStoreFlushStrategy{
					CacheName: cacheName,
					Resources: res,
				}
			case merger.StrategyLogCompacted:
				strat = &merger.LogCompactedFlushStrategy{BatchSize: batchSize}
			case merger.StrategyWindowStream:
				strat = merger.NewWindowStreamStrategy()
			default:
				strat = merger.InlineFlushStrategy{}
			}
			return &processors.LatestMerger{Strategy: strat}, nil
		},
	)

	service.RegisterBatchInput(
		"influxdb",
		service.NewConfigSpec().
			Field(service.NewStringField("url").Description("InfluxDB server URL")).
			Field(service.NewStringField("token").Description("Auth token").Secret()).
			Field(service.NewStringField("org").Description("Organization")).
			Field(service.NewStringField("bucket").Description("Bucket name")).
			Field(service.NewStringListField("pods").Description("List of Pod identifiers to query")).
			Field(service.NewStringField("chunk_duration").Description("Chunk duration per pod (e.g. 2s)").Default("2s")).
			Field(service.NewStringField("lookback").Description("Lookback window (e.g. 10s)").Default("10s")).
			Field(service.NewStringField("tick_interval").Description("Wait between cycles (e.g. 10s); 0 = run one cycle then end").Default("10s")).
			Field(service.NewStringField("resource_map_path").Description("Path to resource_matrix.json")).
			Field(service.NewIntField("batch_size").Description("Max messages per batch").Default(5000)).
			Field(service.NewStringField("measurement").Description("InfluxDB measurement name").Default("telemetry")).
			Field(service.NewIntField("retry_max_attempts").Description("Max retries for InfluxDB query (fault tolerance). 0 = use default 3").Default(3)).
			Field(service.NewStringField("retry_initial_interval").Description("Initial backoff for retry (e.g. 1s). 0 = use default").Default("1s")).
			Field(service.NewStringField("retry_max_interval").Description("Max backoff (e.g. 30s). 0 = use default").Default("30s")).
			Field(service.NewStringField("checkpoint_path").Description("Optional: file path to persist cycle end time (idempotency Option A). Empty = disabled").Default("")),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
			url, _ := conf.FieldString("url")
			token, _ := conf.FieldString("token")
			org, _ := conf.FieldString("org")
			bucket, _ := conf.FieldString("bucket")
			pods, _ := conf.FieldStringList("pods")
			chunkStr, _ := conf.FieldString("chunk_duration")
			lookbackStr, _ := conf.FieldString("lookback")
			tickStr, _ := conf.FieldString("tick_interval")
			resourceMapPath, _ := conf.FieldString("resource_map_path")
			batchSize, _ := conf.FieldInt("batch_size")
			measurement, _ := conf.FieldString("measurement")
			retryMaxAttempts, _ := conf.FieldInt("retry_max_attempts")
			retryInitialStr, _ := conf.FieldString("retry_initial_interval")
			retryMaxStr, _ := conf.FieldString("retry_max_interval")
			checkpointPath, _ := conf.FieldString("checkpoint_path")

			chunkDur, _ := time.ParseDuration(chunkStr)
			if chunkDur <= 0 {
				chunkDur = 2 * time.Second
			}
			lookbackDur, _ := time.ParseDuration(lookbackStr)
			if lookbackDur <= 0 {
				lookbackDur = 10 * time.Second
			}
			tickDur, _ := time.ParseDuration(tickStr)
			if tickDur < 0 {
				tickDur = 0
			}
			retryInitial, _ := time.ParseDuration(retryInitialStr)
			retryMax, _ := time.ParseDuration(retryMaxStr)
			if retryMaxAttempts < 0 {
				retryMaxAttempts = 0
			}

			inp, err := influxdb.New(influxdb.Config{
				URL:                url,
				Token:              token,
				Org:                org,
				Bucket:             bucket,
				Pods:               pods,
				ChunkDuration:      chunkDur,
				Lookback:           lookbackDur,
				TickInterval:       tickDur,
				ResourceMapPath:    resourceMapPath,
				BatchSize:          batchSize,
				Measurement:        measurement,
				RetryMaxAttempts:   retryMaxAttempts,
				RetryInitial:       retryInitial,
				RetryMaxInterval:   retryMax,
				RetryMultiplier:    2.0,
				RetryJitterFactor:  0.1,
				CheckpointPath:     checkpointPath,
			})
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatched(inp), nil
		},
	)

	configPath := os.Getenv("BENTO_CONFIG")
	if configPath == "" {
		configPath = "./config/pipeline.yaml"
	}
	if _, err := os.Stat(configPath); err != nil {
		log.Fatalf("config not found: %s", configPath)
	}

	os.Args = []string{"bento", "--config", configPath}
	service.RunCLI(context.Background())
}
