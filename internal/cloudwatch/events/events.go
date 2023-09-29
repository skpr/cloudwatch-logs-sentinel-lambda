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

func Package(ctx context.Context, svc *cloudwatchlogs.Client, params PackageInput) (PackageOutput, error) {
	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(params.GroupName),
		LogStreamName: aws.String(params.StreamName),
		StartTime:     aws.Int64(params.StartTime),
		EndTime:       aws.Int64(params.EndTime),
	}

	output := PackageOutput{}

	tmpFile := fmt.Sprintf("%s/%s.gz", params.Directory, params.StreamName)

	file, err := os.Create(tmpFile)
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	if err != nil {
		return output, fmt.Errorf("failed to create file, %v", err)
	}

	zipWriter := gzip.NewWriter(file)
	csvwriter := csv.NewWriter(zipWriter)

	for {
		resp, err := svc.GetLogEvents(ctx, input)
		if err != nil {
			return output, fmt.Errorf("failed to get log events, %v", err)
		}

		if len(resp.Events) == 0 {
			break
		}

		output.Count += len(resp.Events)

		for _, event := range resp.Events {
			record := []string{
				time.Unix(*event.Timestamp, 0).String(),
				*event.Message,
			}

			if err := csvwriter.Write(record); err != nil {
				return output, fmt.Errorf("failed to write log event to CSV, %v", err)
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
		return output, fmt.Errorf("failed to flush gzip writer, %v", err)
	}

	if err := zipWriter.Close(); err != nil {
		return output, fmt.Errorf("failed to close gzip writer, %v", err)
	}

	return output, nil
}
