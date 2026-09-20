package tracing

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/common/httpcompat"
)

func Init(ctx context.Context, serviceName string, endpoint string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if strings.TrimSpace(endpoint) == "" {
		tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
		otel.SetTracerProvider(tp)
		return tp.Shutdown, nil
	}

	options := []otlptracehttp.Option{}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		options = append(options, otlptracehttp.WithEndpointURL(endpoint))
	} else {
		options = append(options, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
	}
	if strings.HasPrefix(endpoint, "http://") {
		options = append(options, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, fmt.Errorf("build otel resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

func WrapHTTP(serviceName string, next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, serviceName,
		otelhttp.WithFilter(shouldTraceHTTP),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return httpSpanName(serviceName, r)
		}),
	)
}

func shouldTraceHTTP(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return true
	}
	return !httpcompat.IsOpsProbePath(r.URL.Path)
}

func httpSpanName(serviceName string, r *http.Request) string {
	if r == nil || r.URL == nil {
		return serviceName
	}
	pattern := sagaPattern(serviceName)
	cleanPath := path.Clean("/" + strings.TrimPrefix(r.URL.Path, "/"))
	if cleanPath == "/api/orders" && r.Method == http.MethodPost {
		return pattern + ".order.create"
	}
	if strings.HasPrefix(cleanPath, "/api/orders/") && r.Method == http.MethodGet {
		return pattern + ".order.get"
	}
	if cleanPath == "/api/catalog" && r.Method == http.MethodGet {
		return pattern + ".catalog.get"
	}
	return r.Method + " " + cleanPath
}

func sagaPattern(serviceName string) string {
	if strings.Contains(serviceName, "choreography") {
		return "choreography"
	}
	if strings.Contains(serviceName, "orchestration") {
		return "orchestration"
	}
	return "saga"
}

func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

type HeaderCarrier map[string]string

func (c HeaderCarrier) Get(key string) string {
	return c[key]
}

func (c HeaderCarrier) Set(key string, value string) {
	c[key] = value
}

func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func InjectHeaders(ctx context.Context, headers map[string]string) map[string]string {
	if headers == nil {
		headers = make(map[string]string)
	}
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier(headers))
	return headers
}

func ExtractContext(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier(headers))
}

func NoopShutdown(context.Context) error { return nil }

func NoopWriter() io.Writer { return io.Discard }
