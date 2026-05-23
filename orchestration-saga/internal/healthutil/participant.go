package healthutil

import (
	"context"

	"saga-pattern/common/httpcompat"
)

type HealthChecker interface {
	HealthStatus(context.Context) error
}

func ParticipantHealthProvider(checker HealthChecker, kafkaBrokers string) func(context.Context) httpcompat.HealthResponse {
	return func(ctx context.Context) httpcompat.HealthResponse {
		response := httpcompat.HealthResponse{Status: httpcompat.StatusUp, Components: map[string]httpcompat.HealthComponent{
			"db":    {Status: httpcompat.StatusUp},
			"kafka": {Status: httpcompat.StatusUp, Details: map[string]any{"brokers": kafkaBrokers}},
		}}
		if err := checker.HealthStatus(ctx); err != nil {
			response.Status = "DOWN"
			response.Components["db"] = httpcompat.HealthComponent{Status: "DOWN", Details: map[string]any{"error": err.Error()}}
		}
		return response
	}
}
