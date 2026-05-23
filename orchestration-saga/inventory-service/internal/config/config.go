package config

import sharedconfig "saga-pattern/orchestration-saga/internal/configutil"

const (
	serviceName = "inventory-service-orchestration"
	patternName = "orchestration"
	defaultPort = 8093
)

type RuntimeEnv = sharedconfig.RuntimeEnv

type Config = sharedconfig.Config

func Load() (Config, error) {
	return sharedconfig.Load(sharedconfig.Defaults{
		ServiceName: serviceName,
		Pattern:     patternName,
		DefaultPort: defaultPort,
		DBPort:      5438,
		DBName:      "inventory_db",
	})
}
