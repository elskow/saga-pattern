package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "choreography-shipping-service",
	Pattern:  "choreography",
	HTTPPort: 8084,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
