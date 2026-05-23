package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "shipping-service-choreography",
	Pattern:  "choreography",
	HTTPPort: 8084,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
