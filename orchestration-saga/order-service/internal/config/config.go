package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "order-service-orchestration",
	Pattern:  "orchestration",
	HTTPPort: 8091,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
