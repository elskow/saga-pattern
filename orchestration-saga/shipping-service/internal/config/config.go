package config

import sharedconfig "saga-pattern/orchestration-saga/internal/configutil"

const (
	serviceName = "shipping-service-orchestration"
	patternName = "orchestration"
	defaultPort = 8094
)

type RuntimeEnv = sharedconfig.RuntimeEnv

type Config = sharedconfig.Config

func Load() (Config, error) {
	return sharedconfig.Load(sharedconfig.Defaults{
		ServiceName: serviceName,
		Pattern:     patternName,
		DefaultPort: defaultPort,
		DBPort:      5439,
		DBName:      "shipping_db",
	})
}
