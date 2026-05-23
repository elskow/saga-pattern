package tracing

import (
	"net/http"
	"net/url"
	"testing"
)

func TestHTTPSpanNameUsesBusinessOperationForOrderRoutes(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
		method      string
		path        string
		want        string
	}{
		{name: "choreography create", serviceName: "order-service-choreography", method: http.MethodPost, path: "/api/orders", want: "choreography.order.create"},
		{name: "orchestration create", serviceName: "order-service-orchestration", method: http.MethodPost, path: "/api/orders", want: "orchestration.order.create"},
		{name: "choreography get", serviceName: "order-service-choreography", method: http.MethodGet, path: "/api/orders/order-1", want: "choreography.order.get"},
		{name: "orchestration catalog", serviceName: "order-service-orchestration", method: http.MethodGet, path: "/api/catalog", want: "orchestration.catalog.get"},
		{name: "fallback", serviceName: "payment-service-choreography", method: http.MethodPost, path: "/custom", want: "POST /custom"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := httpSpanName(test.serviceName, &http.Request{Method: test.method, URL: &url.URL{Path: test.path}})
			if got != test.want {
				t.Fatalf("httpSpanName() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestShouldTraceHTTPSuppressesProbeRoutes(t *testing.T) {
	for _, route := range []string{"/actuator/health", "/actuator/prometheus", "/metrics"} {
		if shouldTraceHTTP(&http.Request{URL: &url.URL{Path: route}}) {
			t.Fatalf("shouldTraceHTTP(%q) = true, want false", route)
		}
	}

	if !shouldTraceHTTP(&http.Request{URL: &url.URL{Path: "/api/orders"}}) {
		t.Fatal("shouldTraceHTTP(/api/orders) = false, want true")
	}
}
