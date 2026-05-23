package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	commoncontext "saga-pattern/common/context"
	commontracing "saga-pattern/common/tracing"
)

type RuntimePublisher struct {
	writer *kafkago.Writer
}

func NewRuntimePublisher(brokers string) (*RuntimePublisher, error) {
	parsedBrokers, err := splitBrokers(brokers)
	if err != nil {
		return nil, err
	}

	return &RuntimePublisher{writer: &kafkago.Writer{
		Addr:         kafkago.TCP(parsedBrokers...),
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireOne,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		BatchTimeout: 10 * time.Millisecond,
	}}, nil
}

func (p *RuntimePublisher) Publish(ctx context.Context, topic string, key string, body any) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("runtime publisher is not configured")
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}
	spanAttrs := append([]attribute.KeyValue{
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.operation", "publish"),
		attribute.String("messaging.operation.type", "send"),
		attribute.String("messaging.destination", topic),
		attribute.String("messaging.destination.name", topic),
		attribute.String("messaging.kafka.message_key", key),
	}, payloadAttributes(payload)...)
	spanAttrs = append(spanAttrs, contextAttributes(ctx)...)
	tracer := commontracing.Tracer("common/kafka")
	ctx, span := tracer.Start(ctx, kafkaSpanName("kafka.publish", topic, payload), trace.WithAttributes(spanAttrs...))
	defer span.End()
	kafkaHeaders := kafkaHeadersFromContext(ctx)

	attemptCtx, cancel := publishContext(ctx)
	defer cancel()

	var lastErr error
	backoff := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		if err := p.writer.WriteMessages(attemptCtx, kafkago.Message{Topic: topic, Key: []byte(key), Value: payload, Headers: kafkaHeaders}); err == nil {
			span.AddEvent("kafka.publish.succeeded",
				trace.WithAttributes(
					attribute.String("messaging.destination", topic),
					attribute.String("messaging.kafka.message_key", key),
					attribute.Int("attempt.count", attempt),
				),
			)
			return nil
		} else {
			lastErr = err
			span.AddEvent("kafka.publish.failed",
				trace.WithAttributes(
					attribute.String("messaging.destination", topic),
					attribute.String("messaging.kafka.message_key", key),
					attribute.Int("attempt.count", attempt),
					attribute.String("error", err.Error()),
				),
			)
		}

		if !shouldRetryPublish(attemptCtx, lastErr) {
			break
		}

		select {
		case <-time.After(backoff):
		case <-attemptCtx.Done():
			return fmt.Errorf("write kafka message to %s: %w", topic, lastErr)
		}

		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}

	span.RecordError(lastErr)
	span.SetStatus(codes.Error, lastErr.Error())
	return fmt.Errorf("write kafka message to %s: %w", topic, lastErr)
}

func publishContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, 10*time.Second)
}

func shouldRetryPublish(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}

	var kafkaErr kafkago.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr.Temporary()
	}

	message := err.Error()
	return strings.Contains(message, "Leader Not Available") || strings.Contains(message, "leader election") || strings.Contains(message, "i/o timeout")
}

func (p *RuntimePublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

type MessageHandler func(context.Context, string, string, []byte) error

type SubscriberGroup struct {
	readers []*kafkago.Reader
	handler MessageHandler
	logger  *slog.Logger
}

func NewSubscriberGroup(brokers string, groupID string, topics []string, logger *slog.Logger, handler MessageHandler) (*SubscriberGroup, error) {
	if handler == nil {
		return nil, fmt.Errorf("message handler is required")
	}
	if groupID == "" {
		return nil, fmt.Errorf("consumer group id is required")
	}
	parsedBrokers, err := splitBrokers(brokers)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	readers := make([]*kafkago.Reader, 0, len(topics))
	closeReaders := func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}
	for _, topic := range topics {
		if topic == "" {
			closeReaders()
			return nil, fmt.Errorf("consumer topic is required")
		}
		readers = append(readers, kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     parsedBrokers,
			GroupID:     fmt.Sprintf("%s-%s", groupID, topic),
			Topic:       topic,
			StartOffset: kafkago.FirstOffset,
			MaxWait:     500 * time.Millisecond,
			MinBytes:    1,
			MaxBytes:    10e6,
		}))
	}

	return &SubscriberGroup{readers: readers, handler: handler, logger: logger}, nil
}

func (g *SubscriberGroup) Start(ctx context.Context) {
	for _, reader := range g.readers {
		go g.consumeLoop(ctx, reader)
	}
}

func (g *SubscriberGroup) Close() error {
	var firstErr error
	for _, reader := range g.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (g *SubscriberGroup) consumeLoop(ctx context.Context, reader *kafkago.Reader) {
	tracer := commontracing.Tracer("common/kafka")
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			g.logger.Warn("fetch kafka message failed", "topic", reader.Config().Topic, "error", err)
			time.Sleep(time.Second)
			continue
		}

		headers := make(map[string]string, len(message.Headers))
		for _, header := range message.Headers {
			headers[header.Key] = string(header.Value)
		}
		consumeCtx := commontracing.ExtractContext(ctx, headers)
		consumeCtx = contextFromKafkaHeaders(consumeCtx, headers)
		spanAttrs := append([]attribute.KeyValue{
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "consume"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination", message.Topic),
			attribute.String("messaging.destination.name", message.Topic),
			attribute.String("messaging.kafka.message_key", string(message.Key)),
			attribute.Int("messaging.kafka.partition", message.Partition),
			attribute.Int64("messaging.kafka.offset", message.Offset),
		}, payloadAttributes(message.Value)...)
		spanAttrs = append(spanAttrs, contextAttributes(consumeCtx)...)
		consumeCtx, span := tracer.Start(consumeCtx, kafkaSpanName("kafka.consume", message.Topic, message.Value), trace.WithAttributes(spanAttrs...))
		span.AddEvent("kafka.message.fetched", trace.WithAttributes(kafkaMessageEventAttributes(message)...))
		if err := g.handler(consumeCtx, message.Topic, string(message.Key), message.Value); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			g.logger.Error("handle kafka message failed", "topic", message.Topic, "key", string(message.Key), "error", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		span.AddEvent("kafka.consume.handled", trace.WithAttributes(kafkaMessageEventAttributes(message)...))

		commitCtx, cancelCommit := context.WithTimeout(consumeCtx, 10*time.Second)
		if err := reader.CommitMessages(commitCtx, message); err != nil {
			cancelCommit()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			if ctx.Err() != nil {
				return
			}
			g.logger.Warn("commit kafka message failed", "topic", message.Topic, "key", string(message.Key), "error", err)
			continue
		}
		cancelCommit()
		span.AddEvent("kafka.consume.committed", trace.WithAttributes(kafkaMessageEventAttributes(message)...))
		span.End()
	}
}

func kafkaSpanName(prefix string, topic string, payload []byte) string {
	messageType := messageTypeFromPayload(payload)
	if messageType != "" {
		return prefix + " " + messageType
	}
	if topic != "" {
		return prefix + " " + topic
	}
	return prefix
}

func messageTypeFromPayload(payload []byte) string {
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil || fields == nil {
		return ""
	}
	if messageType, ok := stringField(fields, "type"); ok {
		return messageType
	}
	if eventType, ok := stringField(fields, "eventType"); ok {
		return eventType
	}
	if commandType, ok := stringField(fields, "commandType"); ok {
		return commandType
	}
	return ""
}

func payloadAttributes(payload []byte) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.Int("messaging.message.payload_size_bytes", len(payload))}

	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil || fields == nil {
		return attrs
	}

	if messageType, ok := stringField(fields, "type"); ok {
		attrs = append(attrs,
			attribute.String("message.type", messageType),
			attribute.String("event.type", messageType),
		)
	}
	if eventType, ok := stringField(fields, "eventType"); ok {
		attrs = append(attrs, attribute.String("event.type", eventType))
		if _, hasType := stringField(fields, "type"); !hasType {
			attrs = append(attrs, attribute.String("message.type", eventType))
		}
	}
	if commandType, ok := stringField(fields, "commandType"); ok {
		attrs = append(attrs, attribute.String("saga.command.type", commandType))
	}
	for _, field := range []struct {
		jsonKey string
		attrKey string
	}{
		{jsonKey: "orderId", attrKey: "order.id"},
		{jsonKey: "correlationId", attrKey: "correlation.id"},
		{jsonKey: "paymentId", attrKey: "payment.id"},
		{jsonKey: "reservationId", attrKey: "reservation.id"},
		{jsonKey: "shippingId", attrKey: "shipping.id"},
		{jsonKey: "status", attrKey: "saga.status"},
	} {
		if value, ok := stringField(fields, field.jsonKey); ok {
			attrs = append(attrs, attribute.String(field.attrKey, value))
		}
	}
	return attrs
}

func kafkaHeadersFromContext(ctx context.Context) []kafkago.Header {
	headers := commontracing.InjectHeaders(ctx, nil)
	if data, ok := commoncontext.From(ctx); ok {
		if data.OrderID != "" && data.OrderID != "unknown" {
			headers[commoncontext.HeaderOrderID] = data.OrderID
		}
		if data.RequestID != "" {
			headers[commoncontext.HeaderRequestID] = data.RequestID
		}
		if data.CorrelationID != "" {
			headers[commoncontext.HeaderCorrelationID] = data.CorrelationID
		}
		if data.SagaID != "" {
			headers[commoncontext.HeaderSagaID] = data.SagaID
		}
		if data.SagaType != "" {
			headers[commoncontext.HeaderSagaType] = data.SagaType
		}
		if data.BenchmarkRun != "" {
			headers[commoncontext.HeaderBenchmarkRun] = data.BenchmarkRun
		}
		if data.BenchmarkScene != "" {
			headers[commoncontext.HeaderBenchmarkScene] = data.BenchmarkScene
		}
		if data.BenchmarkPhase != "" {
			headers[commoncontext.HeaderBenchmarkPhase] = data.BenchmarkPhase
		}
	}

	kafkaHeaders := make([]kafkago.Header, 0, len(headers))
	for key, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafkago.Header{Key: key, Value: []byte(value)})
	}
	return kafkaHeaders
}

func contextFromKafkaHeaders(ctx context.Context, headers map[string]string) context.Context {
	data := commoncontext.Data{
		OrderID:        strings.TrimSpace(headers[commoncontext.HeaderOrderID]),
		RequestID:      strings.TrimSpace(headers[commoncontext.HeaderRequestID]),
		CorrelationID:  strings.TrimSpace(headers[commoncontext.HeaderCorrelationID]),
		SagaID:         strings.TrimSpace(headers[commoncontext.HeaderSagaID]),
		SagaType:       strings.TrimSpace(headers[commoncontext.HeaderSagaType]),
		BenchmarkRun:   strings.TrimSpace(headers[commoncontext.HeaderBenchmarkRun]),
		BenchmarkScene: strings.TrimSpace(headers[commoncontext.HeaderBenchmarkScene]),
		BenchmarkPhase: strings.TrimSpace(headers[commoncontext.HeaderBenchmarkPhase]),
	}
	if data.OrderID == "" && data.RequestID == "" && data.CorrelationID == "" && data.SagaID == "" && data.SagaType == "" && data.BenchmarkRun == "" && data.BenchmarkScene == "" && data.BenchmarkPhase == "" {
		return ctx
	}
	return commoncontext.With(ctx, data)
}

func contextAttributes(ctx context.Context) []attribute.KeyValue {
	data, ok := commoncontext.From(ctx)
	if !ok {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, 3)
	if data.OrderID != "" && data.OrderID != "unknown" {
		attrs = append(attrs, attribute.String("order.id", data.OrderID))
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
	return attrs
}

func stringField(fields map[string]any, key string) (string, bool) {
	value, ok := fields[key].(string)
	return value, ok && value != ""
}

func kafkaMessageEventAttributes(message kafkago.Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.destination", message.Topic),
		attribute.String("messaging.kafka.message_key", string(message.Key)),
		attribute.Int("messaging.kafka.partition", message.Partition),
		attribute.Int64("messaging.kafka.offset", message.Offset),
	}
}

func splitBrokers(brokers string) ([]string, error) {
	parts := strings.Split(brokers, ",")
	parsed := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			parsed = append(parsed, trimmed)
		}
	}
	if len(parsed) == 0 {
		return nil, fmt.Errorf("at least one kafka broker is required")
	}
	return parsed, nil
}
