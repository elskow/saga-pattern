package payments

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/common/commands"
	"saga-pattern/common/dto"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/common/testutil"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
	"saga-pattern/orchestration-saga/payment-service/internal/messaging"
	"saga-pattern/orchestration-saga/payment-service/internal/observability"
	"saga-pattern/orchestration-saga/payment-service/internal/repository"
)

func TestProcessPaymentPublishesSuccessReply(t *testing.T) {
	service, repo, publisher := newTestService(t)
	command := commands.NewProcessPaymentCommand("PAY-1", "ORDER-1", "CUST-1", json.Number("1599000"), paymentItems("PROD-001", "1599000"))

	if err := service.HandleProcessPayment(context.Background(), command); err != nil {
		t.Fatalf("handle process payment: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultPaymentRepliesTopic, "ORDER-1")
	reply, ok := publisher.Messages()[0].Body.(commonreplies.PaymentCompletedReply)
	if !ok {
		t.Fatalf("reply type = %T, want PaymentCompletedReply", publisher.Messages()[0].Body)
	}
	if reply.PaymentID != "PAY-1" || reply.OrderID != "ORDER-1" {
		t.Fatalf("completed reply = %+v", reply)
	}
	payment, found, err := repo.GetByPaymentID(context.Background(), "PAY-1")
	if err != nil {
		t.Fatalf("get payment: %v", err)
	}
	if !found || payment.Status != domain.PaymentStatusCompleted {
		t.Fatalf("payment = %+v, found=%v", payment, found)
	}
}

func TestHighAmountNonFixturePublishesSuccessReply(t *testing.T) {
	service, repo, publisher := newTestService(t)
	command := commands.NewProcessPaymentCommand("PAY-HIGH", "ORDER-HIGH", "CUST-1", json.Number("10000.00"), paymentItems("PROD-EXPENSIVE-001", "10000.00"))

	if err := service.HandleProcessPayment(context.Background(), command); err != nil {
		t.Fatalf("handle high amount process payment: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultPaymentRepliesTopic, "ORDER-HIGH")
	if _, ok := publisher.Messages()[0].Body.(commonreplies.PaymentCompletedReply); !ok {
		t.Fatalf("reply type = %T, want PaymentCompletedReply", publisher.Messages()[0].Body)
	}
	payment, found, err := repo.GetByPaymentID(context.Background(), "PAY-HIGH")
	if err != nil {
		t.Fatalf("get payment: %v", err)
	}
	if !found || payment.Status != domain.PaymentStatusCompleted {
		t.Fatalf("payment = %+v, found=%v", payment, found)
	}
}

func TestInsufficientDepositBalancePublishesPaymentFailedReply(t *testing.T) {
	service, repo, publisher := newTestService(t)
	service.SetDepositBalance(big.NewRat(50, 1))
	command := commands.NewProcessPaymentCommand("PAY-BALANCE", "ORDER-BALANCE", "CUST-1", json.Number("1599000"), paymentItems("PROD-001", "1599000"))

	if err := service.HandleProcessPayment(context.Background(), command); err != nil {
		t.Fatalf("handle process payment: %v", err)
	}

	assertSingleReplyMessage(t, publisher, commonkafka.DefaultPaymentRepliesTopic, "ORDER-BALANCE")
	reply, ok := publisher.Messages()[0].Body.(commonreplies.PaymentFailedReply)
	if !ok {
		t.Fatalf("reply type = %T, want PaymentFailedReply", publisher.Messages()[0].Body)
	}
	if reply.Reason != "insufficient deposit balance" {
		t.Fatalf("failure reason = %q", reply.Reason)
	}
	payment, found, err := repo.GetByPaymentID(context.Background(), "PAY-BALANCE")
	if err != nil {
		t.Fatalf("get payment: %v", err)
	}
	if !found || payment.Status != domain.PaymentStatusFailed {
		t.Fatalf("payment = %+v, found=%v", payment, found)
	}
}

func TestRefundPaymentIsIdempotent(t *testing.T) {
	service, repo, publisher := newTestService(t)
	process := commands.NewProcessPaymentCommand("PAY-REFUND", "ORDER-REFUND", "CUST-1", json.Number("799000"), paymentItems("PROD-001", "799000"))
	if err := service.HandleProcessPayment(context.Background(), process); err != nil {
		t.Fatalf("handle process payment: %v", err)
	}
	refund := commands.NewRefundPaymentCommand("PAY-REFUND", "ORDER-REFUND")
	if err := service.HandleRefundPayment(context.Background(), refund); err != nil {
		t.Fatalf("first refund: %v", err)
	}
	firstState, found, err := repo.GetByPaymentID(context.Background(), "PAY-REFUND")
	if err != nil {
		t.Fatalf("get payment after first refund: %v", err)
	}
	if !found || firstState.Status != domain.PaymentStatusRefunded {
		t.Fatalf("payment after first refund = %+v, found=%v", firstState, found)
	}
	if firstState.RefundedAt.IsZero() {
		t.Fatalf("expected refunded timestamp after first refund")
	}
	if err := service.HandleRefundPayment(context.Background(), refund); err != nil {
		t.Fatalf("second refund: %v", err)
	}
	secondState, found, err := repo.GetByPaymentID(context.Background(), "PAY-REFUND")
	if err != nil {
		t.Fatalf("get payment after second refund: %v", err)
	}
	if !found || secondState.Status != domain.PaymentStatusRefunded {
		t.Fatalf("payment after second refund = %+v, found=%v", secondState, found)
	}
	if !secondState.RefundedAt.Equal(firstState.RefundedAt) {
		t.Fatalf("refunded timestamp changed on duplicate refund: first=%s second=%s", firstState.RefundedAt, secondState.RefundedAt)
	}
	if len(publisher.Messages()) != 3 {
		t.Fatalf("published messages = %d, want 3", len(publisher.Messages()))
	}
	for i := 1; i <= 2; i++ {
		reply, ok := publisher.Messages()[i].Body.(commonreplies.PaymentRefundedReply)
		if !ok {
			t.Fatalf("reply[%d] type = %T, want PaymentRefundedReply", i, publisher.Messages()[i].Body)
		}
		if !reply.Success {
			t.Fatalf("reply[%d] success = false", i)
		}
	}
}

func TestRefundPaymentDoesNotRestoreDepositBalanceWhenSaveFails(t *testing.T) {
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	payment, err := domain.NewPayment("PAY-SAVE-FAIL", "ORDER-SAVE-FAIL", "CUST-1", json.Number("25.00"), now)
	if err != nil {
		t.Fatalf("new payment: %v", err)
	}
	if err := payment.MarkCompleted(now); err != nil {
		t.Fatalf("mark completed: %v", err)
	}
	service, err := NewService(&saveFailingPaymentRepository{payment: payment, saveErr: errors.New("save failed")}, messaging.NewReplyPublisher(testutil.NewRecordingPublisher()), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.SetDepositBalance(big.NewRat(0, 1))

	err = service.HandleRefundPayment(context.Background(), commands.NewRefundPaymentCommand("PAY-SAVE-FAIL", "ORDER-SAVE-FAIL"))
	if err == nil || err.Error() != "save failed" {
		t.Fatalf("refund error = %v, want save failed", err)
	}
	if got := service.GetDepositBalance(); got.Cmp(big.NewRat(0, 1)) != 0 {
		t.Fatalf("deposit balance = %s, want 0 after failed save", got.RatString())
	}
}

func TestConcurrentDepositDeductionsDeductOnlySuccessfulPayment(t *testing.T) {
	service, _, _ := newTestService(t)
	service.SetDepositBalance(big.NewRat(100, 1))

	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- service.CheckAndDeductBalance(big.NewRat(75, 1))
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	for ok := range results {
		if ok {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful deductions = %d, want 1", successes)
	}
	if got := service.GetDepositBalance(); got.Cmp(big.NewRat(25, 1)) != 0 {
		t.Fatalf("deposit balance = %s, want 25", got.RatString())
	}
}

func TestConcurrentDuplicateProcessPaymentDeductsBalanceOnce(t *testing.T) {
	service, repo, publisher := newTestService(t)
	service.SetDepositBalance(big.NewRat(100, 1))
	command := commands.NewProcessPaymentCommand("PAY-DUPE", "ORDER-DUPE", "CUST-1", json.Number("75.00"), paymentItems("PROD-001", "75.00"))

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- service.HandleProcessPayment(context.Background(), command)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("handle duplicate process payment: %v", err)
		}
	}

	payment, found, err := repo.GetByPaymentID(context.Background(), "PAY-DUPE")
	if err != nil {
		t.Fatalf("get payment: %v", err)
	}
	if !found || payment.Status != domain.PaymentStatusCompleted {
		t.Fatalf("payment = %+v, found=%v", payment, found)
	}
	if got := service.GetDepositBalance(); got.Cmp(big.NewRat(25, 1)) != 0 {
		t.Fatalf("deposit balance = %s, want 25", got.RatString())
	}
	if len(publisher.Messages()) != 2 {
		t.Fatalf("published messages = %d, want 2", len(publisher.Messages()))
	}
	for i, message := range publisher.Messages() {
		if _, ok := message.Body.(commonreplies.PaymentCompletedReply); !ok {
			t.Fatalf("reply[%d] type = %T, want PaymentCompletedReply", i, message.Body)
		}
	}
}

func paymentItems(productID, price string) []dto.OrderItemRequest {
	return []dto.OrderItemRequest{{ProductID: productID, ProductName: "Test Item", Quantity: 1, Price: json.Number(price)}}
}

func newTestService(t *testing.T) (*Service, repository.Repository, *testutil.RecordingPublisher) {
	t.Helper()
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	db := testutil.OpenPostgres(t, testutil.DefaultPaymentDatabaseURL, "payment_service_test", testutil.Migration{Scope: "orchestration-payment-service", Dir: "orchestration-saga/payment-service/db/migrations"})
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}
	publisher := testutil.NewRecordingPublisher()
	service, err := NewService(repo, messaging.NewReplyPublisher(publisher), metrics)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithClock(func() time.Time { return time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC) })
	return service, repo, publisher
}

func assertSingleReplyMessage(t *testing.T, publisher *testutil.RecordingPublisher, topic, key string) {
	t.Helper()
	if len(publisher.Messages()) != 1 {
		t.Fatalf("published messages = %d, want 1", len(publisher.Messages()))
	}
	message := publisher.Messages()[0]
	if message.Topic != topic || message.Key != key {
		t.Fatalf("published message = (%s,%s), want (%s,%s)", message.Topic, message.Key, topic, key)
	}
}

type saveFailingPaymentRepository struct {
	payment domain.Payment
	saveErr error
}

func (r *saveFailingPaymentRepository) Create(context.Context, domain.Payment) (domain.Payment, error) {
	return domain.Payment{}, errors.New("not implemented")
}

func (r *saveFailingPaymentRepository) Save(context.Context, domain.Payment) error {
	return r.saveErr
}

func (r *saveFailingPaymentRepository) GetByPaymentID(context.Context, string) (domain.Payment, bool, error) {
	return r.payment, true, nil
}

func (r *saveFailingPaymentRepository) PingContext(context.Context) error { return nil }

func (r *saveFailingPaymentRepository) ListPayments(context.Context) ([]domain.Payment, error) {
	return []domain.Payment{r.payment}, nil
}
