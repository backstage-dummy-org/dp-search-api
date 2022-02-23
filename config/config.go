package config

import (
	"encoding/json"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config is the search API handler config
type Config struct {
	AwsRegion                  string        `envconfig:"AWS_REGION"`
	AwsService                 string        `envconfig:"AWS_SERVICE"`
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	ElasticSearchAPIURL        string        `envconfig:"ELASTIC_SEARCH_URL"`
	SignElasticsearchRequests  bool          `envconfig:"SIGN_ELASTICSEARCH_REQUESTS"`
	ZebedeeURL                 string        `envconfig:"ZEBEDEE_URL"`
}

var cfg *Config

// Get configures the application and returns the Config
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		AwsRegion:                  "eu-west-1",
		AwsService:                 "es",
		BindAddr:                   ":23900",
		ElasticSearchAPIURL:        "http://localhost:11200",
		GracefulShutdownTimeout:    5 * time.Second,
		SignElasticsearchRequests:  false,
		HealthCheckCriticalTimeout: 90 * time.Second,
		HealthCheckInterval:        30 * time.Second,
		ZebedeeURL:                 "http://localhost:8082",
	}

	return cfg, envconfig.Process("", cfg)
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Config) String() string {
	data, _ := json.Marshal(config)
	return string(data)
}
