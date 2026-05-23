package orders

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	commonreplies "saga-pattern/common/replies"
	commontracing "saga-pattern/common/tracing"
	sagaRuntime "saga-pattern/orchestration-framework/runtime"
	"saga-pattern/orchestration-saga/order-service/internal/domain"
	"saga-pattern/orchestration-saga/order-service/internal/observability"
	"saga-pattern/orchestration-saga/order-service/internal/repository"
	ordersaga "saga-pattern/orchestration-saga/order-service/internal/saga"
)

var (
	ErrOrderNotFound       = errors.New("order not found")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different order payload")
)

type Clock func() time.Time

type OrchestrationRuntime interface {
	StartSaga(context.Context, sagaRuntime.StartSagaInput[ordersaga.Data]) (string, error)
	ConsumeReply(context.Context, sagaRuntime.ReplyEnvelope) error
	PublishPending(context.Context) error
	View(context.Context, string) (sagaRuntime.View[ordersaga.Data], bool, error)
}

type Service struct {
	repo        repository.Repository
	runtime     OrchestrationRuntime
	catalog     CatalogResolver
	metrics     *observability.Metrics
	clock       Clock
	idGenerator func() string
}

type CatalogResolver interface {
	NormalizeOrderItems(context.Context, []dto.OrderItemRequest) ([]dto.OrderItemRequest, json.Number, error)
}

func NewService(repo repository.Repository, orchestrationRuntime OrchestrationRuntime, catalog CatalogResolver, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if orchestrationRuntime == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if catalog == nil {
		return nil, fmt.Errorf("catalog resolver is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	return &Service{repo: repo, runtime: orchestrationRuntime, catalog: catalog, metrics: metrics, clock: time.Now, idGenerator: commoncontext.NewID}, nil
}

func (s *Service) WithClock(clock Clock) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *Service) WithIDGenerator(generator func() string) {
	if generator != nil {
		s.idGenerator = generator
	}
}

func (s *Service) CreateOrder(ctx context.Context, request dto.OrchestrationCreateOrderRequest, idempotencyKey string) (response dto.OrchestrationCreateOrderAcceptedResponse, err error) {
	ctx, span := commontracing.Tracer("orchestration/order-service").Start(ctx, "orchestration.order.create",
		trace.WithAttributes(
			attribute.String("customer.id", request.CustomerID),
			attribute.Bool("idempotency_key.present", strings.TrimSpace(idempotencyKey) != ""),
			attribute.Int("item.count", len(request.Items)),
		))
	defer finishOrderSpan(span, &err)
	normalizedItems, totalAmount, err := s.catalog.NormalizeOrderItems(ctx, request.Items)
	if err != nil {
		return dto.OrchestrationCreateOrderAcceptedResponse{}, err
	}
	request.Items = normalizedItems
	request.TotalAmount = totalAmount
	orderID := s.nextOrderID(idempotencyKey)
	span.SetAttributes(attribute.String("order.id", orderID), attribute.String("saga.id", orderID))
	paymentID := s.idGenerator()
	reservationID := s.idGenerator()
	shippingID := s.idGenerator()
	if _, err := s.runtime.StartSaga(ctx, sagaRuntime.StartSagaInput[ordersaga.Data]{
		SagaID: orderID,
		Data: ordersaga.Data{
			OrderID:         orderID,
			CustomerID:      request.CustomerID,
			PaymentID:       paymentID,
			ReservationID:   reservationID,
			ShippingID:      shippingID,
			CorrelationID:   commoncontext.ResolveCorrelationID(""),
			ShippingAddress: request.ShippingAddress,
			TotalAmount:     request.TotalAmount,
			Items:           append([]dto.OrderItemRequest(nil), request.Items...),
		},
	}); err != nil {
		if errors.Is(err, sagaRuntime.ErrSagaAlreadyExists) && idempotencyKey != "" {
			view, ok, viewErr := s.runtime.View(ctx, orderID)
			if viewErr != nil {
				return dto.OrchestrationCreateOrderAcceptedResponse{}, viewErr
			}
			if !ok || !sameOrchestrationRequest(view.Data, request) {
				return dto.OrchestrationCreateOrderAcceptedResponse{}, ErrIdempotencyConflict
			}
			span.SetAttributes(attribute.String("order.result", "duplicate_accepted"))
			return dto.NewOrchestrationCreateOrderAcceptedResponse(orderID, request), nil
		}
		return dto.OrchestrationCreateOrderAcceptedResponse{}, err
	}
	if err := s.repo.AcknowledgeAcceptedLifecycle(ctx); err != nil {
		return dto.OrchestrationCreateOrderAcceptedResponse{}, err
	}
	s.metrics.RecordOrderCreated()
	span.SetAttributes(attribute.String("order.result", "saga_started"))
	span.AddEvent("orchestration.saga.started", trace.WithAttributes(attribute.String("saga.id", orderID)))
	return dto.NewOrchestrationCreateOrderAcceptedResponse(orderID, request), nil
}

func (s *Service) nextOrderID(idempotencyKey string) string {
	key := strings.TrimSpace(idempotencyKey)
	if key == "" {
		return s.idGenerator()
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:16])
}

func (s *Service) GetOrder(ctx context.Context, orderID string) (dto.OrderResponse, error) {
	order, ok, err := s.repo.GetFinalized(ctx, orderID)
	if err != nil {
		return dto.OrderResponse{}, err
	}
	if !ok {
		return dto.OrderResponse{}, ErrOrderNotFound
	}
	return order.Response(), nil
}

func sameOrchestrationRequest(existing ordersaga.Data, incoming dto.OrchestrationCreateOrderRequest) bool {
	if existing.CustomerID != incoming.CustomerID || existing.ShippingAddress != incoming.ShippingAddress || existing.TotalAmount.String() != incoming.TotalAmount.String() || len(existing.Items) != len(incoming.Items) {
		return false
	}
	for i := range existing.Items {
		left := existing.Items[i]
		right := incoming.Items[i]
		if left.ProductID != right.ProductID || left.ProductName != right.ProductName || left.Quantity != right.Quantity || left.Price.String() != right.Price.String() {
			return false
		}
	}
	return true
}

func (s *Service) ListOrders(ctx context.Context) ([]dto.OrderResponse, error) {
	orders, err := s.repo.ListFinalized(ctx)
	if err != nil {
		return nil, err
	}
	return responsesFromOrders(orders), nil
}

func (s *Service) ListOrdersByCustomer(ctx context.Context, customerID string) ([]dto.OrderResponse, error) {
	orders, err := s.repo.ListFinalizedByCustomer(ctx, customerID)
	if err != nil {
		return nil, err
	}
	return responsesFromOrders(orders), nil
}

func (s *Service) HandleReply(ctx context.Context, envelope sagaRuntime.ReplyEnvelope) (err error) {
	ctx, span := commontracing.Tracer("orchestration/order-service").Start(ctx, "orchestration.order.handle_reply",
		trace.WithAttributes(
			attribute.String("saga.id", envelope.SagaID),
			attribute.String("reply.topic", envelope.Topic),
			attribute.Int("messaging.message.payload_size_bytes", len(envelope.Payload)),
		))
	defer finishOrderSpan(span, &err)
	if err := s.runtime.ConsumeReply(ctx, envelope); err != nil {
		return err
	}
	reply, err := commonreplies.DecodeSagaReply(bytes.NewReader(envelope.Payload))
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.String("reply.type", reply.ReplyType()))
	s.recordCompensationMetric(reply.ReplyType())
	runtimeView, ok, err := s.runtime.View(ctx, envelope.SagaID)
	if err != nil || !ok {
		return err
	}
	// The runtime remains the source of truth for in-flight orchestration state.
	// The orders table only becomes queryable once a terminal runtime view is
	// materialized into the finalized projection.
	if runtimeView.State != domain.StatusCompleted && runtimeView.State != domain.StatusCancelled {
		span.SetAttributes(attribute.String("saga.state", string(runtimeView.State)), attribute.String("order.result", "in_flight"))
		return nil
	}
	finalizedOrder, err := s.repo.UpsertFinalizedFromRuntimeView(ctx, runtimeView, s.clock().UTC())
	if err != nil {
		return err
	}
	duration := finalizedOrder.UpdatedAt.Sub(finalizedOrder.CreatedAt)
	if finalizedOrder.Status == domain.StatusCompleted {
		s.metrics.RecordOrderCompleted(duration)
		span.SetAttributes(attribute.String("saga.state", string(runtimeView.State)), attribute.String("order.result", "completed"))
		span.AddEvent("orchestration.saga.completed", trace.WithAttributes(attribute.String("saga.id", envelope.SagaID)))
		return nil
	}
	s.metrics.RecordOrderFailed(duration)
	span.SetAttributes(attribute.String("saga.state", string(runtimeView.State)), attribute.String("order.result", "cancelled"))
	span.AddEvent("orchestration.saga.cancelled", trace.WithAttributes(attribute.String("saga.id", envelope.SagaID)))
	return nil
}

func finishOrderSpan(span trace.Span, err *error) {
	if err != nil && *err != nil {
		span.RecordError(*err)
		span.SetStatus(codes.Error, (*err).Error())
	}
	span.End()
}

func (s *Service) PublishPending(ctx context.Context) error {
	return s.runtime.PublishPending(ctx)
}

func (s *Service) recordCompensationMetric(replyType string) {
	switch replyType {
	case commonreplies.TypePaymentRefunded:
		s.metrics.RecordPaymentCompensation()
	case commonreplies.TypeInventoryReleased:
		s.metrics.RecordInventoryCompensation()
	case commonreplies.TypeShippingCancelled:
		s.metrics.RecordShippingCompensation()
	}
}

func responsesFromOrders(orders []domain.Order) []dto.OrderResponse {
	responses := make([]dto.OrderResponse, 0, len(orders))
	for _, order := range orders {
		responses = append(responses, order.Response())
	}
	return responses
}
