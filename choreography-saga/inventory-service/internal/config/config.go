package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "choreography-inventory-service",
	Pattern:  "choreography",
	HTTPPort: 8083,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
