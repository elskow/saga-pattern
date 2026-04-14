package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "orchestration-order-service",
	Pattern:  "orchestration",
	HTTPPort: 8085,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
