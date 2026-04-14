package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	env "github.com/caarlos0/env/v11"

	commonconfig "saga-pattern/common/config"
)

const (
	serviceName = "orchestration-shipping-service"
	patternName = "orchestration"
	defaultPort = 8093
)

type RuntimeEnv struct {
	DatabaseURL     string
	KafkaBrokers    string
	LogLevel        string
	ShutdownTimeout time.Duration
}

type Config struct {
	ServiceName    string
	Pattern        string
	HTTPPort       int
	HealthPath     string
	PrometheusPath string
	Runtime        RuntimeEnv
}

type rawEnv struct {
	ServerPort            int           `env:"SERVER_PORT" envDefault:"8093"`
	DatabaseURL           string        `env:"DATABASE_URL"`
	DBHost                string        `env:"DB_HOST" envDefault:"localhost"`
	DBPort                int           `env:"DB_PORT" envDefault:"5439"`
	DBName                string        `env:"DB_NAME" envDefault:"shipping_db"`
	DBUsername            string        `env:"DB_USERNAME" envDefault:"postgres"`
	DBPassword            string        `env:"DB_PASSWORD" envDefault:"postgres"`
	KafkaBrokers          string        `env:"KAFKA_BROKERS"`
	KafkaBootstrapServers string        `env:"KAFKA_BOOTSTRAP_SERVERS" envDefault:"localhost:9093"`
	LogLevel              string        `env:"LOG_LEVEL" envDefault:"INFO"`
	ShutdownTimeout       time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"5s"`
}

func Load() (Config, error) {
	parsed := rawEnv{}
	if err := env.Parse(&parsed); err != nil {
		return Config{}, fmt.Errorf("parse runtime env: %w", err)
	}

	databaseURL := strings.TrimSpace(parsed.DatabaseURL)
	if databaseURL == "" {
		databaseURL = buildDatabaseURL(parsed)
	}
	kafkaBrokers := strings.TrimSpace(parsed.KafkaBrokers)
	if kafkaBrokers == "" {
		kafkaBrokers = strings.TrimSpace(parsed.KafkaBootstrapServers)
	}
	if kafkaBrokers == "" {
		return Config{}, fmt.Errorf("kafka brokers are required")
	}

	httpPort := parsed.ServerPort
	if httpPort <= 0 {
		httpPort = defaultPort
	}

	return Config{
		ServiceName:    serviceName,
		Pattern:        patternName,
		HTTPPort:       httpPort,
		HealthPath:     commonconfig.DefaultHealthPath,
		PrometheusPath: commonconfig.DefaultPrometheusPath,
		Runtime: RuntimeEnv{
			DatabaseURL:     databaseURL,
			KafkaBrokers:    kafkaBrokers,
			LogLevel:        parsed.LogLevel,
			ShutdownTimeout: parsed.ShutdownTimeout,
		},
	}, nil
}

func (c Config) Address() string {
	return fmt.Sprintf(":%d", c.HTTPPort)
}

func buildDatabaseURL(parsed rawEnv) string {
	return (&url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(parsed.DBUsername, parsed.DBPassword),
		Host:   fmt.Sprintf("%s:%d", parsed.DBHost, parsed.DBPort),
		Path:   parsed.DBName,
		RawQuery: url.Values{
			"sslmode": []string{"disable"},
		}.Encode(),
	}).String()
}
