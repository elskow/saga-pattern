package orders

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/choreography-saga/order-service/internal/domain"
	"saga-pattern/choreography-saga/order-service/internal/messaging"
	"saga-pattern/choreography-saga/order-service/internal/observability"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
	commontracing "saga-pattern/common/tracing"
)

var (
	ErrOrderNotFound       = errors.New("order not found")
	ErrIdempotencyConflict = repository.ErrIdempotencyConflict
)

type Clock func() time.Time

type Service struct {
	repo      repository.Repository
	publisher messaging.OrderTopicPublisher
	catalog   CatalogResolver
	metrics   *observability.Metrics
	clock     Clock
}

type CatalogResolver interface {
	NormalizeOrderItems(context.Context, []dto.OrderItemRequest) ([]dto.OrderItemRequest, json.Number, error)
}

func NewService(repo repository.Repository, publisher messaging.OrderTopicPublisher, catalog CatalogResolver, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if catalog == nil {
		return nil, fmt.Errorf("catalog resolver is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	return &Service{repo: repo, publisher: publisher, catalog: catalog, metrics: metrics, clock: time.Now}, nil
}

func (s *Service) WithClock(clock Clock) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *Service) CreateOrder(ctx context.Context, request dto.ChoreographyCreateOrderRequest, idempotencyKey string) (dto.OrderResponse, bool, error) {
	ctx, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.create",
		trace.WithAttributes(
			attribute.String("customer.id", request.CustomerID),
			attribute.String("idempotency_key", idempotencyKey),
			attribute.Int("item.count", len(request.Items)),
		))
	defer span.End()
	now := s.clock().UTC()
	orderID := commoncontext.NewID()
	span.SetAttributes(attribute.String("order.id", orderID))
	normalizedItems, _, err := s.catalog.NormalizeOrderItems(ctx, request.Items)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return dto.OrderResponse{}, false, err
	}
	request.Items = normalizedItems
	order, err := domain.NewOrderFromRequest(orderID, request, idempotencyKey, now)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return dto.OrderResponse{}, false, err
	}

	created := true
	if idempotencyKey == "" {
		order, err = s.repo.Create(ctx, order)
	} else {
		order, created, err = s.repo.CreateIfAbsent(ctx, idempotencyKey, order)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return dto.OrderResponse{}, false, err
	}
	span.SetAttributes(attribute.Bool("created", created))

	if created {
		data := commoncontext.Current(ctx)
		data.OrderID = order.OrderID
		data.CorrelationID = order.CorrelationID
		ctx = commoncontext.With(ctx, data)
		span.SetAttributes(attribute.String("correlation.id", order.CorrelationID))
		if err := s.PublishPendingOrderEvents(ctx); err != nil {
			span.RecordError(err)
			span.AddEvent("order_created_publish_deferred", trace.WithAttributes(attribute.String("error", err.Error())))
		} else {
			span.AddEvent("order_created_published")
		}
		s.metrics.RecordOrderCreated()
	}

	return order.Response(), created, nil
}

func (s *Service) GetOrder(ctx context.Context, orderID string) (dto.OrderResponse, error) {
	order, ok, err := s.repo.Get(ctx, orderID)
	if err != nil {
		return dto.OrderResponse{}, err
	}
	if !ok {
		return dto.OrderResponse{}, ErrOrderNotFound
	}
	return order.Response(), nil
}

func (s *Service) ListOrders(ctx context.Context) ([]dto.OrderResponse, error) {
	orders, err := s.repo.List(ctx)
	if err != nil {
		return nil, err
	}
	responses := make([]dto.OrderResponse, 0, len(orders))
	for _, order := range orders {
		responses = append(responses, order.Response())
	}
	return responses, nil
}

func (s *Service) PublishPendingOrderEvents(ctx context.Context) error {
	messages, err := s.repo.ClaimPendingOrderEvents(ctx, 100)
	if err != nil {
		return err
	}
	for _, message := range messages {
		if message.EventType != "OrderCreated" {
			if err := s.repo.MarkOrderEventPublishFailed(ctx, message.ID, "unsupported order outbox event type: "+message.EventType); err != nil {
				return err
			}
			continue
		}
		var event events.OrderCreatedEvent
		if err := json.Unmarshal([]byte(message.PayloadJSON), &event); err != nil {
			if markErr := s.repo.MarkOrderEventPublishFailed(ctx, message.ID, err.Error()); markErr != nil {
				return markErr
			}
			return err
		}
		if err := s.publisher.PublishOrderCreated(ctx, message.OrderID, event); err != nil {
			if markErr := s.repo.MarkOrderEventPublishFailed(ctx, message.ID, err.Error()); markErr != nil {
				return markErr
			}
			return err
		}
		if err := s.repo.MarkOrderEventPublished(ctx, message.ID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	ctx = events.ContextWithMetadata(ctx, event)
	ctx, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.handle_event",
		trace.WithAttributes(attribute.String("event.type", event.EventType())))
	defer span.End()
	switch e := event.(type) {
	case events.PaymentCompletedEvent:
		return s.foldPaymentCompleted(ctx, e)
	case events.InventoryReservedEvent:
		return s.foldInventoryReserved(ctx, e)
	case events.ShippingScheduledEvent:
		return s.foldShippingScheduled(ctx, e)
	case events.PaymentFailedEvent:
		return s.foldTerminalFailure(ctx, e.OrderID, "payment-failed:"+e.OrderID, "payment", "Payment failed: "+e.Reason)
	case events.InventoryReservationFailedEvent:
		return s.foldTerminalFailure(ctx, e.OrderID, "inventory-failed:"+e.OrderID, "inventory", "Inventory reservation failed: "+e.Reason)
	case events.ShippingFailedEvent:
		return s.foldTerminalFailure(ctx, e.OrderID, "shipping-failed:"+e.OrderID, "shipping", "Shipping failed: "+e.Reason)
	case events.PaymentRefundedEvent:
		return s.recordCompensationAcknowledged(ctx, "payment-refunded:"+e.OrderID)
	case events.InventoryReleasedEvent:
		return s.recordCompensationAcknowledged(ctx, "inventory-released:"+e.OrderID)
	default:
		span.RecordError(fmt.Errorf("unsupported choreography event %T", event))
		span.SetStatus(codes.Error, "unsupported event")
		return fmt.Errorf("unsupported choreography event %T", event)
	}
}

func (s *Service) foldPaymentCompleted(ctx context.Context, event events.PaymentCompletedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.payment_completed",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("payment.id", event.PaymentID)))
	defer span.End()
	eventKey := "payment-completed:" + event.PaymentID
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(attribute.Bool("event.processed", !claimed))
		return err
	}
	defer s.releaseProcessedEventOnError(ctx, eventKey, &err)
	order, err := s.loadOrderForEvent(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if isTerminalStatus(order.Status) {
		span.AddEvent("order_progress_ignored_after_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
		return nil
	}
	order.MarkPaymentCompleted(event.PaymentID, s.clock())
	span.AddEvent("order_payment_marked_completed", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
	if err := s.saveFoldedOrder(ctx, order); err != nil {
		return err
	}
	return nil
}

func (s *Service) foldInventoryReserved(ctx context.Context, event events.InventoryReservedEvent) (err error) {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.inventory_reserved",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("reservation.id", event.ReservationID)))
	defer span.End()
	eventKey := "inventory-reserved:" + event.ReservationID
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(attribute.Bool("event.processed", !claimed))
		return err
	}
	defer s.releaseProcessedEventOnError(ctx, eventKey, &err)
	order, err := s.loadOrderForEvent(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if isTerminalStatus(order.Status) {
		span.AddEvent("order_progress_ignored_after_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
		return nil
	}
	order.MarkInventoryReserved(event.ReservationID, s.clock())
	span.AddEvent("order_inventory_marked_reserved", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
	if err := s.saveFoldedOrder(ctx, order); err != nil {
		return err
	}
	return nil
}

func (s *Service) foldShippingScheduled(ctx context.Context, event events.ShippingScheduledEvent) (err error) {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.shipping_scheduled",
		trace.WithAttributes(attribute.String("order.id", event.OrderID), attribute.String("shipping.id", event.ShippingID)))
	defer span.End()
	eventKey := "shipping-scheduled:" + event.ShippingID
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(attribute.Bool("event.processed", !claimed))
		return err
	}
	defer s.releaseProcessedEventOnError(ctx, eventKey, &err)
	order, err := s.loadOrderForEvent(ctx, event.OrderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if isTerminalStatus(order.Status) {
		span.AddEvent("order_progress_ignored_after_terminal", trace.WithAttributes(attribute.String("order.status", string(order.Status))))
		return nil
	}
	order.MarkCompleted(event.ShippingID, event.TrackingNumber, s.clock())
	span.AddEvent("order_marked_completed", trace.WithAttributes(attribute.String("shipping.id", event.ShippingID)))
	if err := s.saveFoldedOrder(ctx, order); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	s.metrics.RecordOrderCompleted(order.UpdatedAt.Sub(order.CreatedAt))
	return nil
}

func (s *Service) foldTerminalFailure(ctx context.Context, orderID string, eventKey string, failureType string, reason string) (err error) {
	_, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.failure",
		trace.WithAttributes(attribute.String("order.id", orderID), attribute.String("event.key", eventKey), attribute.String("failure.type", failureType)))
	defer span.End()
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(attribute.Bool("event.processed", !claimed))
		return err
	}
	defer s.releaseProcessedEventOnError(ctx, eventKey, &err)
	order, err := s.loadOrderForEvent(ctx, orderID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	order.MarkCancelled(reason, s.clock())
	span.AddEvent("order_marked_cancelled")
	if err := s.saveFoldedOrder(ctx, order); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	s.metrics.RecordOrderFailed(order.UpdatedAt.Sub(order.CreatedAt))
	return nil
}

func (s *Service) recordCompensationAcknowledged(ctx context.Context, eventKey string) error {
	claimed, err := s.claimProcessedEvent(ctx, eventKey)
	if err != nil || !claimed {
		return err
	}
	return nil
}

func (s *Service) claimProcessedEvent(ctx context.Context, eventKey string) (bool, error) {
	return s.repo.TryMarkProcessedEvent(ctx, eventKey)
}

func (s *Service) releaseProcessedEventOnError(ctx context.Context, eventKey string, err *error) {
	if err != nil && *err != nil {
		_ = s.repo.DeleteProcessedEvent(ctx, eventKey)
	}
}

func (s *Service) loadOrderForEvent(ctx context.Context, orderID string) (domain.Order, error) {
	order, ok, err := s.repo.Get(ctx, orderID)
	if err != nil {
		return domain.Order{}, err
	}
	if !ok {
		return domain.Order{}, ErrOrderNotFound
	}
	return order, nil
}

func (s *Service) saveFoldedOrder(ctx context.Context, order domain.Order) error {
	return s.repo.Save(ctx, order)
}

func isTerminalStatus(status dto.OrderStatus) bool {
	return status == dto.OrderStatusCompleted || status == dto.OrderStatusCancelled
}
