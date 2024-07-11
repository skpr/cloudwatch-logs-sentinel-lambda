package stream

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// CloudWatchClient is a client for CloudWatch.
type CloudWatchClient interface {
	DescribeLogStreams(ctx context.Context, params *cloudwatchlogs.DescribeLogStreamsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogStreamsOutput, error)
}

// GetLogStreams returns a list of log streams for a given log group.
func GetLogStreams(ctx context.Context, svc CloudWatchClient, group string, startTime int64) ([]types.LogStream, error) {
	var streams []types.LogStream

	input := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: aws.String(group),
		OrderBy:      types.OrderByLastEventTime,
		Descending:   aws.Bool(true),
	}

	for {
		resp, err := svc.DescribeLogStreams(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to describe log streams, %v", err)
		}

		var n []types.LogStream

		for _, stream := range resp.LogStreams {
			if stream.LastEventTimestamp == nil {
				continue
			}

			if *stream.LastEventTimestamp < startTime {
				continue
			}

			n = append(n, stream)
		}

		if len(n) == 0 {
			break
		}

		streams = append(streams, n...)

		if resp.NextToken == nil {
			break
		}

		input.NextToken = resp.NextToken
	}

	return streams, nil
}
