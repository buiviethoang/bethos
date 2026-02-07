package main

import (
	"bethos/internal/input/influxdb"
	"bethos/internal/merger"
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
			Field(service.NewIntField("seed").Description("Random seed for reproducible data; 0 = time-based").Default(0)),
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

			return &processors.CSVGenerator{
				FilePath:        filePath,
				Count:           count,
				BulkBufferSize:  bufBytes,
				NumVincodes:     numVincodes,
				ResourceMapPath: resourceMapPath,
				Seed:            int64(seed),
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
		service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchProcessor, error) {
			return &processors.Aggregator{}, nil
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
		"uppercase_go",
		service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.Processor, error) {
			return &processors.UppercaseProcessor{}, nil
		},
	)

	service.RegisterProcessor(
		"latest_merger",
		service.NewConfigSpec().
			Field(service.NewStringField("strategy").Description("inline = emit batch to output; state_store = write to cache, separate publisher reads and emits").Default("inline")).
			Field(service.NewStringField("cache").Description("Cache resource name for state_store strategy").Default("")).
			Field(service.NewStringField("cache_index_key").Description("Cache key for list of vincodes (state_store)").Default("")).
			Field(service.NewStringField("cache_prefix").Description("Cache key prefix per device (state_store)").Default("")),
		func(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			strategyName, _ := conf.FieldString("strategy")
			cacheName, _ := conf.FieldString("cache")
			indexKey, _ := conf.FieldString("cache_index_key")
			prefix, _ := conf.FieldString("cache_prefix")

			var strat merger.FlushStrategy
			switch strategyName {
			case merger.StrategyStateStore:
				strat = &merger.StateStoreFlushStrategy{
					CacheName: cacheName,
					IndexKey:  indexKey,
					Prefix:    prefix,
					Resources: res,
				}
			default:
				strat = merger.InlineFlushStrategy{}
			}
			return &processors.LatestMerger{Strategy: strat}, nil
		},
	)

	service.RegisterProcessor(
		"latest_state_publisher",
		service.NewConfigSpec().
			Field(service.NewStringField("cache").Description("Cache resource name (same as merger state_store cache)")).
			Field(service.NewStringField("cache_index_key").Default("")).
			Field(service.NewStringField("cache_prefix").Default("")),
		func(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			cacheName, _ := conf.FieldString("cache")
			indexKey, _ := conf.FieldString("cache_index_key")
			prefix, _ := conf.FieldString("cache_prefix")
			return &processors.LatestStatePublisher{
				CacheName: cacheName,
				IndexKey:  indexKey,
				Prefix:    prefix,
				Resources: res,
			}, nil
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
			Field(service.NewStringField("resource_map_path").Description("Path to resource_matrix.json")).
			Field(service.NewIntField("batch_size").Description("Max messages per batch").Default(5000)).
			Field(service.NewStringField("measurement").Description("InfluxDB measurement name").Default("telemetry")),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
			url, _ := conf.FieldString("url")
			token, _ := conf.FieldString("token")
			org, _ := conf.FieldString("org")
			bucket, _ := conf.FieldString("bucket")
			pods, _ := conf.FieldStringList("pods")
			chunkStr, _ := conf.FieldString("chunk_duration")
			lookbackStr, _ := conf.FieldString("lookback")
			resourceMapPath, _ := conf.FieldString("resource_map_path")
			batchSize, _ := conf.FieldInt("batch_size")
			measurement, _ := conf.FieldString("measurement")

			chunkDur, _ := time.ParseDuration(chunkStr)
			if chunkDur <= 0 {
				chunkDur = 2 * time.Second
			}
			lookbackDur, _ := time.ParseDuration(lookbackStr)
			if lookbackDur <= 0 {
				lookbackDur = 10 * time.Second
			}

			inp, err := influxdb.New(influxdb.Config{
				URL:             url,
				Token:           token,
				Org:             org,
				Bucket:          bucket,
				Pods:            pods,
				ChunkDuration:   chunkDur,
				Lookback:        lookbackDur,
				ResourceMapPath: resourceMapPath,
				BatchSize:       batchSize,
				Measurement:     measurement,
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
