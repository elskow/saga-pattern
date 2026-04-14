package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "choreography-order-service",
	Pattern:  "choreography",
	HTTPPort: 8081,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
