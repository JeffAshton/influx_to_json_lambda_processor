package main

import (
	"context"

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

func handler(ctx context.Context, kinesisEvent kinesisEvent) (kinesisResponse, error) {

	handler := influx.NewMetricHandler()
	parser := influx.NewParser(handler)
	serializer, _ := json.NewSerializer(0)

	responseRecords := make([]kinesisEventResponse, len(kinesisEvent.Records))

	for index, record := range kinesisEvent.Records {

		metrics, parseErr := parser.Parse(record.Data)
		if parseErr == nil {

			jsonBytes, jsonErr := serializer.Serialize(metrics[0])
			if jsonErr == nil {

				responseRecords[index] = kinesisEventResponse{record.RecordID, "Ok", jsonBytes}

			} else {
				responseRecords[index] = kinesisEventResponse{record.RecordID, "ProcessingFailed", nil}
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
