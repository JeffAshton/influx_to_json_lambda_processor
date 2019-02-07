package main

import (
	"context"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/influxdata/telegraf"
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

		measurementWhitelist := make(map[string]bool)

		names := strings.Split(whitelist, ",")
		for _, name := range names {
			measurementWhitelist[name] = true
		}
	}
}

func shouldProcess(m telegraf.Metric) bool {

	if len(measurementWhitelist) == 0 {
		return true
	}

	measurementName := m.Name()
	_, ok := measurementWhitelist[measurementName]
	return ok
}

func handler(ctx context.Context, kinesisEvent kinesisEvent) (kinesisResponse, error) {

	handler := influx.NewMetricHandler()
	parser := influx.NewParser(handler)
	serializer, _ := json.NewSerializer(0)

	responseRecords := make([]kinesisEventResponse, len(kinesisEvent.Records))

	for index, record := range kinesisEvent.Records {

		metrics, parseErr := parser.Parse(record.Data)
		if parseErr == nil {

			metric := metrics[0]

			if shouldProcess(metric) {

				jsonBytes, jsonErr := serializer.Serialize(metric)

				if jsonErr == nil {
					responseRecords[index] = kinesisEventResponse{record.RecordID, "Ok", jsonBytes}

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
	return response, nil
}

func main() {
	lambda.Start(handler)
}
