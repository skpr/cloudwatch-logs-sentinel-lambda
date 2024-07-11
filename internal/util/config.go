package util

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config used by this application.
type Config struct {
	GroupName          string        `mapstructure:"CLOUDWATCH_LOGS_SENTINEL_GROUP_NAME"`
	DiscoveryStart     time.Duration `mapstructure:"CLOUDWATCH_LOGS_SENTINEL_DISCOVERY_START"`
	Start              time.Duration `mapstructure:"CLOUDWATCH_LOGS_SENTINEL_START"`
	End                time.Duration `mapstructure:"CLOUDWATCH_LOGS_SENTINEL_END"`
	BucketName         string        `mapstructure:"CLOUDWATCH_LOGS_SENTINEL_BUCKET_NAME"`
	BucketPrefix       string        `mapstructure:"CLOUDWATCH_LOGS_SENTINEL_BUCKET_PREFIX"`
	TemporaryDirectory string        `mapstructure:"CLOUDWATCH_LOGS_SENTINEL_TEMPORARY_DIRECTORY"`
	InjectFields       string        `mapstructure:"CLOUDWATCH_LOGS_SENTINEL_INJECT_FIELDS"`
}

// Validate validates the config.
func (c Config) Validate() []string {
	var errors []string

	if c.GroupName == "" {
		errors = append(errors, "CLOUDWATCH_LOGS_SENTINEL_GROUP_NAME is a required variable")
	}

	if c.DiscoveryStart.Milliseconds() >= c.Start.Milliseconds() {
		errors = append(errors, "CLOUDWATCH_LOGS_SENTINEL_DISCOVERY_START should be a duration before CLOUDWATCH_LOGS_SENTINEL_START")
	}

	if c.Start.Milliseconds() >= c.End.Milliseconds() {
		errors = append(errors, "CLOUDWATCH_LOGS_SENTINEL_START should be a duration before CLOUDWATCH_LOGS_SENTINEL_END")
	}

	if c.BucketName == "" {
		errors = append(errors, "CLOUDWATCH_LOGS_SENTINEL_BUCKET_NAME is a required variable")
	}

	if c.BucketPrefix == "" {
		errors = append(errors, "CLOUDWATCH_LOGS_SENTINEL_BUCKET_PREFIX is a required variable")
	}

	if c.TemporaryDirectory == "" {
		errors = append(errors, "CLOUDWATCH_LOGS_SENTINEL_TEMPORARY_DIRECTORY is a required variable")
	}

	return errors
}

// LoadConfig reads configuration from file or environment variables.
func LoadConfig(path string) (Config, error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("defaults")
	viper.SetConfigType("env")
	viper.AutomaticEnv()

	var config Config

	err := viper.ReadInConfig()
	if err != nil {
		return config, fmt.Errorf("failed to read config: %w", err)
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return config, err
}
