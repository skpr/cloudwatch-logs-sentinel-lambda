package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/skpr/cloudwatch-logs-sentinel-lambda/internal/cloudwatch/events"
	streamutils "github.com/skpr/cloudwatch-logs-sentinel-lambda/internal/cloudwatch/stream"
)

var (
	GroupName    = "/skpr/skpr-dcs/dcs/dev/cloudfront"
	StartTime    = time.Now().Add(-time.Hour * 1).UTC().UnixMilli()
	EndTime      = time.Now().UTC().UnixMilli()
	BucketName   = ""
	BucketPrefix = ""
)

const (
	// LogKeyCloudWatchLogsGroupName is the name of the log stream.
	LogKeyCloudWatchLogsGroupName = "cloudwatch_logs_group_name"
	// LogKeyCloudWatchLogsStreamName is the name of the log stream.
	LogKeyCloudWatchLogsStreamName = "cloudwatch_logs_stream_name"
	// LogKeyCloudWatchLogsStreamStartTime is the start time of the log stream.
	LogKeyCloudWatchLogsStreamStartTime = "cloudwatch_logs_stream_start_time"
	// LogKeyCloudWatchLogsStreamEndTime is the finish time of the log stream.
	LogKeyCloudWatchLogsStreamEndTime = "cloudwatch_logs_stream_end_time"
	// LogKeyCloudWatchLogsStreamLogCount is the number of log events in the stream.
	LogKeyCloudWatchLogsStreamLogCount = "cloudwatch_logs_stream_log_count"
	// LogKeyTemporaryFilePath is the path to the temporary file.
	LogKeyTemporaryFilePath = "temporary_file_path"
	// LogKeyS3BucketName is the name of the S3 bucket.
	LogKeyS3BucketName = "s3_bucket_name"
	// LogKeyS3BucketKey is the key of the S3 object.
	LogKeyS3BucketKey = "s3_bucket_key"
)

func hello(ctx context.Context) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	logger.LogAttrs(ctx, slog.LevelInfo, "Starting function",
		slog.String(LogKeyCloudWatchLogsGroupName, GroupName),
		slog.Int64(LogKeyCloudWatchLogsStreamStartTime, StartTime),
		slog.Int64(LogKeyCloudWatchLogsStreamEndTime, EndTime),
		slog.String(LogKeyS3BucketName, BucketName))

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Client used to download and package CloudWatch Logs.
	svc := cloudwatchlogs.NewFromConfig(cfg)

	// Client for pushing package to S3.
	uploader := s3manager.NewUploader(s3.NewFromConfig(cfg))

	// This is used to create a unique upload file name.
	now := time.Now().UTC().String()

	logger.LogAttrs(ctx, slog.LevelInfo, "Getting CloudWatch log streams")

	streams, err := streamutils.GetLogStreams(ctx, svc, GroupName, StartTime)
	if err != nil {
		return fmt.Errorf("failed to get log streams, %v", err)
	}

	for _, stream := range streams {
		logger.LogAttrs(ctx, slog.LevelInfo, "Packaging log events",
			slog.String(LogKeyCloudWatchLogsGroupName, GroupName),
			slog.String(LogKeyCloudWatchLogsStreamName, *stream.LogStreamName))

		output, err := events.Package(ctx, svc, events.PackageInput{
			GroupName:  GroupName,
			StreamName: *stream.LogStreamName,
			StartTime:  StartTime,
			EndTime:    EndTime,
		})
		if err != nil {
			return fmt.Errorf("failed to push log events, %w", err)
		}

		logger.LogAttrs(ctx, slog.LevelInfo, "Successfully packaged log events to filesystem",
			slog.String(LogKeyCloudWatchLogsGroupName, GroupName),
			slog.String(LogKeyCloudWatchLogsStreamName, *stream.LogStreamName),
			slog.String(LogKeyTemporaryFilePath, output.FilePath),
			slog.Int(LogKeyCloudWatchLogsStreamLogCount, output.Count))

		file, err := os.Open(output.FilePath)
		if err != nil {
			return fmt.Errorf("failed to open file %q, %w", output.FilePath, err)
		}

		key := fmt.Sprintf("%s/%s/%s.gz", BucketPrefix, *stream.LogStreamName, now)

		_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(BucketName),
			Key:    aws.String(key),
			Body:   file,
		})

		logger.LogAttrs(ctx, slog.LevelInfo, "Finished pushing log events to S3 bucket",
			slog.String(LogKeyCloudWatchLogsGroupName, GroupName),
			slog.String(LogKeyCloudWatchLogsStreamName, *stream.LogStreamName),
			slog.Int(LogKeyCloudWatchLogsStreamLogCount, output.Count),
			slog.String(LogKeyTemporaryFilePath, output.FilePath),
			slog.String(LogKeyS3BucketName, BucketName),
			slog.String(LogKeyS3BucketKey, key))
	}

	return nil
}

func main() {
	lambda.Start(hello)
}
