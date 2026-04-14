package orders

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	commonreplies "saga-pattern/common/replies"
	frameworkruntime "saga-pattern/orchestration-framework/runtime"
	"saga-pattern/orchestration-saga/order-service/internal/domain"
	"saga-pattern/orchestration-saga/order-service/internal/observability"
	"saga-pattern/orchestration-saga/order-service/internal/repository"
)

var ErrOrderNotFound = errors.New("order not found")

type Clock func() time.Time

type OrchestrationRuntime interface {
	StartSaga(context.Context, frameworkruntime.StartSagaInput) (string, error)
	ConsumeReply(context.Context, frameworkruntime.ReplyEnvelope) error
	PublishPending(context.Context) error
	Snapshot(context.Context, string) (frameworkruntime.Snapshot, bool, error)
}

type Service struct {
	repo        repository.Repository
	runtime     OrchestrationRuntime
	metrics     *observability.Metrics
	clock       Clock
	idGenerator func() string
}

func NewService(repo repository.Repository, orchestrationRuntime OrchestrationRuntime, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if orchestrationRuntime == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	return &Service{repo: repo, runtime: orchestrationRuntime, metrics: metrics, clock: time.Now, idGenerator: commoncontext.NewID}, nil
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

func (s *Service) CreateOrder(ctx context.Context, request dto.OrchestrationCreateOrderRequest) (dto.OrchestrationCreateOrderAcceptedResponse, error) {
	now := s.clock().UTC()
	orderID := s.idGenerator()
	paymentID := s.idGenerator()
	reservationID := s.idGenerator()
	shippingID := s.idGenerator()
	if _, err := s.runtime.StartSaga(ctx, frameworkruntime.StartSagaInput{
		SagaID: orderID,
		Data: frameworkruntime.SagaData{
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
		return dto.OrchestrationCreateOrderAcceptedResponse{}, err
	}
	if err := s.repo.SaveAccepted(ctx, domain.NewAccepted(orderID, request, paymentID, reservationID, shippingID, now)); err != nil {
		return dto.OrchestrationCreateOrderAcceptedResponse{}, err
	}
	s.metrics.RecordOrderCreated()
	return dto.NewOrchestrationCreateOrderAcceptedResponse(orderID, request), nil
}

func (s *Service) GetOrder(ctx context.Context, orderID string) (dto.OrderResponse, error) {
	order, ok, err := s.repo.GetVisible(ctx, orderID)
	if err != nil {
		return dto.OrderResponse{}, err
	}
	if !ok {
		return dto.OrderResponse{}, ErrOrderNotFound
	}
	return order.Response(), nil
}

func (s *Service) ListOrders(ctx context.Context) ([]dto.OrderResponse, error) {
	orders, err := s.repo.ListVisible(ctx)
	if err != nil {
		return nil, err
	}
	return responsesFromOrders(orders), nil
}

func (s *Service) ListOrdersByCustomer(ctx context.Context, customerID string) ([]dto.OrderResponse, error) {
	orders, err := s.repo.ListVisibleByCustomer(ctx, customerID)
	if err != nil {
		return nil, err
	}
	return responsesFromOrders(orders), nil
}

func (s *Service) HandleReply(ctx context.Context, envelope frameworkruntime.ReplyEnvelope) error {
	if err := s.runtime.ConsumeReply(ctx, envelope); err != nil {
		return err
	}
	reply, err := commonreplies.DecodeSagaReply(bytes.NewReader(envelope.Payload))
	if err != nil {
		return err
	}
	s.recordCompensationMetric(reply.ReplyType())
	snapshot, ok, err := s.runtime.Snapshot(ctx, envelope.SagaID)
	if err != nil || !ok {
		return err
	}
	if snapshot.State != domain.StatusCompleted && snapshot.State != domain.StatusCancelled {
		return nil
	}
	order, err := s.repo.FinalizeFromSnapshot(ctx, snapshot, s.clock().UTC())
	if err != nil {
		return err
	}
	duration := order.UpdatedAt.Sub(order.CreatedAt)
	if order.Status == domain.StatusCompleted {
		s.metrics.RecordOrderCompleted(duration)
		return nil
	}
	s.metrics.RecordOrderFailed(duration)
	return nil
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
