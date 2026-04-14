package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
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
		Addr:                   kafkago.TCP(parsedBrokers...),
		Balancer:               &kafkago.LeastBytes{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafkago.RequireOne,
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		BatchTimeout:           250 * time.Millisecond,
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

	attemptCtx, cancel := publishContext(ctx)
	defer cancel()

	var lastErr error
	backoff := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		if err := p.writer.WriteMessages(attemptCtx, kafkago.Message{Topic: topic, Key: []byte(key), Value: payload}); err == nil {
			return nil
		} else {
			lastErr = err
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

const defaultTopicPartitions = 6

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
	if err := ensureTopicsExist(parsedBrokers, topics); err != nil {
		return nil, err
	}

	readers := make([]*kafkago.Reader, 0, len(topics))
	for _, topic := range topics {
		if topic == "" {
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

func ensureTopicsExist(brokers []string, topics []string) error {
	controllerConn, err := dialController(brokers)
	if err != nil {
		return fmt.Errorf("dial kafka controller: %w", err)
	}
	defer controllerConn.Close()

	configs := make([]kafkago.TopicConfig, 0, len(topics))
	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		configs = append(configs, kafkago.TopicConfig{Topic: topic, NumPartitions: defaultTopicPartitions, ReplicationFactor: 1})
	}
	if len(configs) == 0 {
		return nil
	}

	if err := controllerConn.CreateTopics(configs...); err != nil && !isTopicExistsError(err) {
		return fmt.Errorf("create kafka topics: %w", err)
	}
	return nil
}

func dialController(brokers []string) (*kafkago.Conn, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		for _, broker := range brokers {
			conn, err := kafkago.Dial("tcp", broker)
			if err != nil {
				lastErr = err
				continue
			}
			controller, err := conn.Controller()
			conn.Close()
			if err != nil {
				lastErr = err
				continue
			}
			controllerAddress := net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port))
			controllerConn, err := kafkago.Dial("tcp", controllerAddress)
			if err != nil {
				lastErr = err
				continue
			}
			return controllerConn, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no kafka brokers available")
	}
	return nil, lastErr
}

func isTopicExistsError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "Topic with this name already exists") || strings.Contains(message, "TOPIC_ALREADY_EXISTS")
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

		if err := g.handler(ctx, message.Topic, string(message.Key), message.Value); err != nil {
			g.logger.Error("handle kafka message failed", "topic", message.Topic, "key", string(message.Key), "error", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			if ctx.Err() != nil {
				return
			}
			g.logger.Warn("commit kafka message failed", "topic", message.Topic, "key", string(message.Key), "error", err)
			continue
		}
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
