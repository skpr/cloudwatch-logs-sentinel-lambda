package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	config, err := LoadConfig("testdata")
	assert.NoError(t, err)
	assert.Equal(t, "/skpr/test/things", config.GroupName)
	assert.Equal(t, -time.Hour*1, config.Start)
	assert.Equal(t, time.Duration(0), config.End)
	assert.Equal(t, "skpr-test", config.BucketName)
	assert.Equal(t, "/my/test/prefix", config.BucketPrefix)
}

func TestValidate(t *testing.T) {
	var tests = []struct {
		name   string
		config Config
		fails  bool
	}{
		{
			name:  "Missing all config",
			fails: true,
		},
		{
			name: "Missing bucket name and prefix config",
			config: Config{
				GroupName: "/skpr/test/things",
			},
			fails: true,
		},
		{
			name: "Missing bucket prefix config",
			config: Config{
				GroupName:  "/skpr/test/things",
				BucketName: "skpr-test",
			},
			fails: true,
		},
		{
			name: "Start needs to be before end",
			config: Config{
				GroupName:    "/skpr/test/things",
				BucketName:   "skpr-test",
				BucketPrefix: "/my/test/prefix",
			},
			fails: true,
		},
		{
			name: "Missing bucket prefix config",
			config: Config{
				GroupName:    "/skpr/test/things",
				BucketName:   "skpr-test",
				BucketPrefix: "/my/test/prefix",
				Start:        -time.Hour * 1,
			},
			fails: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans := tt.config.Validate()
			if len(ans) > 0 != tt.fails {
				t.Errorf("got %s, want %v", ans, tt.fails)
			}
		})
	}
}
