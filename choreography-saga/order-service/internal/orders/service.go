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
	"saga-pattern/choreography-saga/order-service/internal/observability"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	commontracing "saga-pattern/common/tracing"
)

var (
	ErrOrderNotFound       = errors.New("order not found")
	ErrIdempotencyConflict = repository.ErrIdempotencyConflict
)

type Clock func() time.Time

type CatalogResolver interface {
	NormalizeOrderItems(context.Context, []dto.OrderItemRequest) ([]dto.OrderItemRequest, json.Number, error)
}

type Service struct {
	repo        repository.Repository
	participant participantAdapter
	catalog     CatalogResolver
	metrics     *observability.Metrics
	clock       Clock
}

func NewService(repo repository.Repository, participant participantAdapter, catalog CatalogResolver, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if participant == nil {
		return nil, fmt.Errorf("participant adapter is required")
	}
	if catalog == nil {
		return nil, fmt.Errorf("catalog resolver is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	return &Service{repo: repo, participant: participant, catalog: catalog, metrics: metrics, clock: time.Now}, nil
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

	hook := s.buildCreateOrderHook(order)

	created := true
	if idempotencyKey == "" {
		order, err = s.repo.Create(ctx, order, hook)
	} else {
		order, created, err = s.repo.CreateIfAbsent(ctx, idempotencyKey, order, hook)
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
		s.participant.TriggerImmediatePublish(ctx)
		span.AddEvent("order_created_publish_triggered")
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

func (s *Service) CancelOrder(ctx context.Context, orderId string) error {
	ctx, span := commontracing.Tracer("choreography/order-service").Start(ctx, "choreography.order.cancel",
		trace.WithAttributes(attribute.String("order.id", orderId)))
	defer span.End()

	if err := s.repo.CancelOrder(ctx, orderId); err != nil {
		if err.Error() == "order not found" {
			return ErrOrderNotFound
		}
		return err
	}
	return nil
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
