package main

import (
	"bethos/processors"
	"context"
	"log"
	"os"

	_ "github.com/warpstreamlabs/bento/public/components/all"
	"github.com/warpstreamlabs/bento/public/service"
)

func main() {

	service.RegisterProcessor(
		"csv_generator",
		service.NewConfigSpec().
			Field(service.NewStringField("file_path")).
			Field(service.NewIntField("records_per_tick")),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {

			filePath, err := conf.FieldString("file_path")
			if err != nil {
				return nil, err
			}

			count, err := conf.FieldInt("records_per_tick")
			if err != nil {
				return nil, err
			}

			return &processors.CSVGenerator{
				FilePath: filePath,
				Count:    count,
			}, nil
		},
	)

	service.RegisterProcessor(
		"csv_reader",
		service.NewConfigSpec().
			Field(service.NewStringField("file_path")).
			Field(service.NewIntField("batch_size")),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {

			filePath, err := conf.FieldString("file_path")
			if err != nil {
				return nil, err
			}

			batchSize, err := conf.FieldInt("batch_size")
			if err != nil {
				return nil, err
			}

			return &processors.CSVReader{
				FilePath: filePath,
				Batch:    batchSize,
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

	if _, err := os.Stat("./config/pipeline.yaml"); err != nil {
		log.Fatal("pipeline.yaml not found")
	}

	os.Args = []string{"bento", "--config", "./config/pipeline.yaml"}
	service.RunCLI(context.Background())
}
