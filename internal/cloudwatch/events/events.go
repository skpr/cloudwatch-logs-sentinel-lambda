package events

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

type PackageInput struct {
	GroupName  string
	StreamName string
	StartTime  int64
	EndTime    int64
	Directory  string
}

type PackageOutput struct {
	FilePath string
	Count    int
}

func Package(ctx context.Context, svc *cloudwatchlogs.Client, params PackageInput) (PackageOutput, bool, error) {
	var hasEvents bool

	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(params.GroupName),
		LogStreamName: aws.String(params.StreamName),
		StartTime:     aws.Int64(params.StartTime),
		EndTime:       aws.Int64(params.EndTime),
		StartFromHead: aws.Bool(true),
	}

	output := PackageOutput{
		FilePath: fmt.Sprintf("%s/%s.gz", params.Directory, params.StreamName),
	}

	file, err := os.Create(output.FilePath)
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	if err != nil {
		return output, hasEvents, fmt.Errorf("failed to create file, %v", err)
	}

	zipWriter := gzip.NewWriter(file)
	csvwriter := csv.NewWriter(zipWriter)

	// https://github.com/Azure/Azure-Sentinel/blob/master/DataConnectors/AWS-S3/CloudWatchLanbdaFunction.py#L57C132-L57C143
	csvwriter.Comma = ' '

	for {
		resp, err := svc.GetLogEvents(ctx, input)
		if err != nil {
			return output, hasEvents, fmt.Errorf("failed to get log events, %v", err)
		}

		count := len(resp.Events)

		// If we have events AND we have not marked this before.
		if count > 0 && !hasEvents {
			hasEvents = true
		}

		// We don't have any more logs to write.
		if count == 0 {
			break
		}

		output.Count += len(resp.Events)

		for _, event := range resp.Events {
			record := []string{
				time.UnixMilli(*event.Timestamp).Format("2006-01-02T15:04:05.000Z"),
			}

			record = append(record, *event.Message)

			if err := csvwriter.Write(record); err != nil {
				return output, hasEvents, fmt.Errorf("failed to write log event to CSV, %v", err)
			}
		}

		// If you have reached the end of the stream, it returns the same token you passed in.
		// We need to set and exit early here for another loop to know if this is the end.
		if input.NextToken == nil {
			input.NextToken = resp.NextForwardToken
			break
		}

		// If you have reached the end of the stream, CloudWatch Logs returns the same token you passed in.
		if *resp.NextForwardToken == *input.NextToken {
			break
		}

		input.NextToken = resp.NextForwardToken
	}

	csvwriter.Flush()

	if err := zipWriter.Flush(); err != nil {
		return output, hasEvents, fmt.Errorf("failed to flush gzip writer, %v", err)
	}

	if err := zipWriter.Close(); err != nil {
		return output, hasEvents, fmt.Errorf("failed to close gzip writer, %v", err)
	}

	return output, hasEvents, nil
}
