package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "order-service-choreography",
	Pattern:  "choreography",
	HTTPPort: 8081,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
