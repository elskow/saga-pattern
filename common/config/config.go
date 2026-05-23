package config

import (
	"fmt"
	"time"

	env "github.com/caarlos0/env/v11"
)

const (
	DefaultHealthPath     = "/actuator/health"
	DefaultPrometheusPath = "/actuator/prometheus"
)

type ServiceSpec struct {
	Name     string
	Pattern  string
	HTTPPort int
}

type RuntimeEnv struct {
	ServerPort          int           `env:"SERVER_PORT"`
	DatabaseURL         string        `env:"DATABASE_URL,required"`
	KafkaBrokers        string        `env:"KAFKA_BROKERS,required"`
	InventoryServiceURL string        `env:"INVENTORY_SERVICE_URL"`
	LogLevel            string        `env:"LOG_LEVEL" envDefault:"INFO"`
	ShutdownTimeout     time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"5s"`
	SagaStepTimeout     time.Duration `env:"SAGA_STEP_TIMEOUT"`
	SagaTimeout         time.Duration `env:"SAGA_TIMEOUT"`
	OTLPEndpoint        string        `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OTELServiceName     string        `env:"OTEL_SERVICE_NAME"`
}

type ServiceConfig struct {
	ServiceName    string
	Pattern        string
	HTTPPort       int
	HealthPath     string
	PrometheusPath string
	Runtime        RuntimeEnv
}

func Load(spec ServiceSpec) (ServiceConfig, error) {
	return loadWithEnvironment(spec, nil)
}

func loadWithEnvironment(spec ServiceSpec, environment map[string]string) (ServiceConfig, error) {
	if spec.Name == "" {
		return ServiceConfig{}, fmt.Errorf("service spec name is required")
	}
	if spec.Pattern == "" {
		return ServiceConfig{}, fmt.Errorf("service spec pattern is required")
	}
	if spec.HTTPPort <= 0 {
		return ServiceConfig{}, fmt.Errorf("service spec HTTP port must be positive")
	}

	runtime := RuntimeEnv{}
	options := env.Options{}
	if environment != nil {
		options.Environment = environment
	}
	if err := env.ParseWithOptions(&runtime, options); err != nil {
		return ServiceConfig{}, fmt.Errorf("parse runtime env: %w", err)
	}
	httpPort := spec.HTTPPort
	if runtime.ServerPort > 0 {
		httpPort = runtime.ServerPort
	}

	return ServiceConfig{
		ServiceName:    spec.Name,
		Pattern:        spec.Pattern,
		HTTPPort:       httpPort,
		HealthPath:     DefaultHealthPath,
		PrometheusPath: DefaultPrometheusPath,
		Runtime:        runtime,
	}, nil
}

func (c ServiceConfig) Address() string {
	return fmt.Sprintf(":%d", c.HTTPPort)
}
