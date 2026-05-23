package config

import commonconfig "saga-pattern/common/config"

var spec = commonconfig.ServiceSpec{
	Name:     "inventory-service-choreography",
	Pattern:  "choreography",
	HTTPPort: 8083,
}

func Load() (commonconfig.ServiceConfig, error) {
	return commonconfig.Load(spec)
}
