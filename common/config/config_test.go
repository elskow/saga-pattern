package config

import (
	"strings"
	"testing"
	"time"
)

func TestMissingRequiredEnvFailsFast(t *testing.T) {
	t.Parallel()

	spec := ServiceSpec{Name: "config-test-service", Pattern: "choreography", HTTPPort: 8081}

	tests := []struct {
		name        string
		environment map[string]string
		wantErr     string
	}{
		{
			name:        "missing database url",
			environment: map[string]string{"KAFKA_BROKERS": "localhost:9092"},
			wantErr:     "DATABASE_URL",
		},
		{
			name:        "missing kafka brokers",
			environment: map[string]string{"DATABASE_URL": "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable"},
			wantErr:     "KAFKA_BROKERS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := loadWithEnvironment(spec, tt.environment)
			if err == nil {
				t.Fatal("expected config validation error, got nil")
			}
			if !strings.Contains(err.Error(), "parse runtime env") {
				t.Fatalf("expected wrapped parse error, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error mentioning %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := loadWithEnvironment(
		ServiceSpec{Name: "config-test-service", Pattern: "orchestration", HTTPPort: 8091},
		map[string]string{
			"DATABASE_URL":  "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable",
			"KAFKA_BROKERS": "localhost:9093",
		},
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ServiceName != "config-test-service" || cfg.Pattern != "orchestration" {
		t.Fatalf("unexpected service identity: %+v", cfg)
	}
	if cfg.HealthPath != DefaultHealthPath || cfg.PrometheusPath != DefaultPrometheusPath {
		t.Fatalf("unexpected actuator defaults: health=%q metrics=%q", cfg.HealthPath, cfg.PrometheusPath)
	}
	if cfg.Address() != ":8091" {
		t.Fatalf("unexpected listen address: %q", cfg.Address())
	}
	if cfg.Runtime.LogLevel != "INFO" {
		t.Fatalf("unexpected log level default: %q", cfg.Runtime.LogLevel)
	}
	if cfg.Runtime.ShutdownTimeout != 5*time.Second {
		t.Fatalf("unexpected shutdown timeout default: %s", cfg.Runtime.ShutdownTimeout)
	}
}

func TestLoadHonorsServerPortOverride(t *testing.T) {
	t.Parallel()

	cfg, err := loadWithEnvironment(
		ServiceSpec{Name: "config-test-service", Pattern: "orchestration", HTTPPort: 8091},
		map[string]string{
			"SERVER_PORT":   "8088",
			"DATABASE_URL":  "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable",
			"KAFKA_BROKERS": "localhost:9093",
		},
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Address() != ":8088" {
		t.Fatalf("unexpected overridden listen address: %q", cfg.Address())
	}
}

func TestLoadHonorsSagaTimeoutOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := loadWithEnvironment(
		ServiceSpec{Name: "config-test-service", Pattern: "orchestration", HTTPPort: 8091},
		map[string]string{
			"DATABASE_URL":      "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable",
			"KAFKA_BROKERS":     "localhost:9093",
			"SAGA_STEP_TIMEOUT": "45s",
			"SAGA_TIMEOUT":      "180s",
		},
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Runtime.SagaStepTimeout != 45*time.Second {
		t.Fatalf("unexpected saga step timeout: %s", cfg.Runtime.SagaStepTimeout)
	}
	if cfg.Runtime.SagaTimeout != 180*time.Second {
		t.Fatalf("unexpected saga timeout: %s", cfg.Runtime.SagaTimeout)
	}
}
