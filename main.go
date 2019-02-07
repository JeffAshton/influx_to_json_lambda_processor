package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	"github.com/influxdata/telegraf/plugins/serializers/json"
)

type kinesisEventRecord struct {
	RecordID string `json:"recordId"`
	Data     []byte `json:"data"`
}

type kinesisEvent struct {
	Records []kinesisEventRecord `json:"records"`
}

type kinesisEventResponse struct {
	RecordID string `json:"recordId"`
	Result   string `json:"result"`
	Data     []byte `json:"data"`
}

type kinesisResponse struct {
	Records []kinesisEventResponse `json:"records"`
}

var measurementWhitelist map[string]bool

func init() {

	whitelist := os.Getenv("MEASUREMENTS_WHITELIST")
	if len(whitelist) > 0 {

		measurementWhitelist = make(map[string]bool)

		names := strings.Split(whitelist, ",")
		for _, name := range names {
			measurementWhitelist[name] = true
		}

		fmt.Print("Measurement Whitelist")
		seperator := ": "
		for key := range measurementWhitelist {
			fmt.Print(seperator)
			fmt.Print(key)
			seperator = ", "
		}
		fmt.Println()
	}
}

func shouldProcess(metricName string) bool {

	if len(measurementWhitelist) == 0 {
		return true
	}

	process := measurementWhitelist[metricName]
	return process
}

func handler(ctx context.Context, kinesisEvent kinesisEvent) (kinesisResponse, error) {

	handler := influx.NewMetricHandler()
	parser := influx.NewParser(handler)
	serializer, _ := json.NewSerializer(0)

	processed := make(map[string]int)
	responseRecords := make([]kinesisEventResponse, len(kinesisEvent.Records))

	for index, record := range kinesisEvent.Records {

		metrics, parseErr := parser.Parse(record.Data)
		if parseErr == nil {

			metric := metrics[0]
			metricName := metric.Name()

			if shouldProcess(metricName) {

				jsonBytes, jsonErr := serializer.Serialize(metric)

				if jsonErr == nil {
					responseRecords[index] = kinesisEventResponse{record.RecordID, "Ok", jsonBytes}
					processed[metricName]++

				} else {
					responseRecords[index] = kinesisEventResponse{record.RecordID, "ProcessingFailed", nil}
				}

			} else {
				responseRecords[index] = kinesisEventResponse{record.RecordID, "Dropped", nil}
			}

		} else {
			responseRecords[index] = kinesisEventResponse{record.RecordID, "ProcessingFailed", nil}
		}
	}

	response := kinesisResponse{responseRecords}
	fmt.Println("Processed", processed)

	return response, nil
}

func main() {
	lambda.Start(handler)
}
