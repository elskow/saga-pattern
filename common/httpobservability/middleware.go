package httpobservability

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/httpcompat"
)

type Options struct {
	ServiceName string
	Pattern     string
	Logger      *slog.Logger
}

func Wrap(next http.Handler, opts Options) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		data := commoncontext.Current(r.Context())
		headerData := commoncontext.FromHeaders(r.Header)
		if strings.TrimSpace(r.Header.Get(commoncontext.HeaderOrderID)) != "" {
			data.OrderID = headerData.OrderID
		}
		data.RequestID = commoncontext.ResolveRequestID(r.Header.Get(commoncontext.HeaderRequestID), data.RequestID)
		data.CorrelationID = commoncontext.ResolveCorrelationID(r.Header.Get(commoncontext.HeaderCorrelationID), data.CorrelationID)
		if strings.TrimSpace(r.Header.Get(commoncontext.HeaderSagaID)) != "" {
			data.SagaID = headerData.SagaID
		}
		if strings.TrimSpace(r.Header.Get(commoncontext.HeaderSagaType)) != "" {
			data.SagaType = headerData.SagaType
		}
		if strings.TrimSpace(r.Header.Get(commoncontext.HeaderBenchmarkRun)) != "" {
			data.BenchmarkRun = headerData.BenchmarkRun
		}
		if strings.TrimSpace(r.Header.Get(commoncontext.HeaderBenchmarkScene)) != "" {
			data.BenchmarkScene = headerData.BenchmarkScene
		}
		if strings.TrimSpace(r.Header.Get(commoncontext.HeaderBenchmarkPhase)) != "" {
			data.BenchmarkPhase = headerData.BenchmarkPhase
		}
		if strings.TrimSpace(r.Header.Get(commoncontext.HeaderSuiteLabel)) != "" {
			data.SuiteLabel = headerData.SuiteLabel
		}
		r = r.WithContext(commoncontext.With(r.Context(), data))
		w.Header().Set(commoncontext.HeaderRequestID, data.RequestID)
		recorder := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(recorder, r)

		// Probe paths: serve health/metrics, but no access log / span noise (metrics scrape stays).
		if httpcompat.IsOpsProbePath(r.URL.Path) {
			return
		}

		duration := time.Since(start)
		route := routePattern(r)
		span := trace.SpanFromContext(r.Context())
		traceID := ""
		spanID := ""
		if spanContext := span.SpanContext(); spanContext.IsValid() {
			traceID = spanContext.TraceID().String()
			spanID = spanContext.SpanID().String()
		}

		attrs := []attribute.KeyValue{
			attribute.String("service.name", opts.ServiceName),
			attribute.String("saga.pattern", opts.Pattern),
			attribute.String("http.request.method", r.Method),
			attribute.String("http.route", route),
			attribute.Int("http.response.status_code", recorder.statusCode),
			attribute.Int64("http.response.body.size", recorder.bytesWritten),
			attribute.Int64("http.request.duration_ms", duration.Milliseconds()),
		}
		if data.RequestID != "" {
			attrs = append(attrs, attribute.String("request.id", data.RequestID))
		}
		if data.CorrelationID != "" {
			attrs = append(attrs, attribute.String("correlation.id", data.CorrelationID))
		}
		if data.BenchmarkRun != "" {
			attrs = append(attrs, attribute.String("benchmark.run_label", data.BenchmarkRun))
		}
		if data.BenchmarkScene != "" {
			attrs = append(attrs, attribute.String("benchmark.scenario", data.BenchmarkScene))
		}
		if data.BenchmarkPhase != "" {
			attrs = append(attrs, attribute.String("benchmark.phase", data.BenchmarkPhase))
		}
		if data.SuiteLabel != "" {
			attrs = append(attrs, attribute.String("suite_label", data.SuiteLabel))
		}
		span.SetAttributes(attrs...)
		span.AddEvent("http.request.completed", trace.WithAttributes(attrs...))

		level := slog.LevelInfo
		if recorder.statusCode >= http.StatusInternalServerError {
			level = slog.LevelError
		} else if recorder.statusCode >= http.StatusBadRequest {
			level = slog.LevelWarn
		}

		logAttrs := []slog.Attr{
			slog.String("service", opts.ServiceName),
			slog.String("pattern", opts.Pattern),
			slog.String("method", r.Method),
			slog.String("route", route),
			slog.String("path", r.URL.Path),
			slog.Int("status", recorder.statusCode),
			slog.Int64("bytes", recorder.bytesWritten),
			slog.Int64("duration_ms", duration.Milliseconds()),
			slog.String("trace_id", traceID),
			slog.String("span_id", spanID),
		}
		if data.RequestID != "" {
			logAttrs = append(logAttrs, slog.String("request_id", data.RequestID))
		}
		if data.CorrelationID != "" {
			logAttrs = append(logAttrs, slog.String("correlation_id", data.CorrelationID))
		}
		if data.BenchmarkRun != "" {
			logAttrs = append(logAttrs, slog.String("benchmark_run_label", data.BenchmarkRun))
		}
		if data.BenchmarkScene != "" {
			logAttrs = append(logAttrs, slog.String("benchmark_scenario", data.BenchmarkScene))
		}
		if data.BenchmarkPhase != "" {
			logAttrs = append(logAttrs, slog.String("benchmark_phase", data.BenchmarkPhase))
		}
		if data.SuiteLabel != "" {
			logAttrs = append(logAttrs, slog.String("suite_label", data.SuiteLabel))
		}
		logger.LogAttrs(r.Context(), level, "http request completed", logAttrs...)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	wroteHeader  bool
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	if r.wroteHeader {
		return
	}
	r.statusCode = statusCode
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(body []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(body)
	r.bytesWritten += int64(n)
	return n, err
}

func routePattern(r *http.Request) string {
	if pattern := chi.RouteContext(r.Context()).RoutePattern(); pattern != "" {
		return pattern
	}
	return r.URL.Path
}

func (r *responseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func (r *responseRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *responseRecorder) ReadFrom(src io.Reader) (int64, error) {
	if readerFrom, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		if !r.wroteHeader {
			r.WriteHeader(http.StatusOK)
		}
		n, err := readerFrom.ReadFrom(src)
		r.bytesWritten += n
		return n, err
	}
	return io.Copy(responseRecorderWriter{recorder: r}, src)
}

func (r *responseRecorder) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := r.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

type responseRecorderWriter struct {
	recorder *responseRecorder
}

func (w responseRecorderWriter) Write(body []byte) (int, error) {
	return w.recorder.Write(body)
}
