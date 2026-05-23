package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "payment-service-choreography",
	Pattern:  "choreography",
	HTTPPort: 8082,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
