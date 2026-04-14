package parity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"saga-pattern/common/dto"
)

const (
	healthPath = "/actuator/health"
	ordersPath = "/api/orders"

	defaultPollAttempts   = 60
	defaultPollInterval   = 500 * time.Millisecond
	defaultHTTPTimeout    = 10 * time.Second
	defaultShippingStreet = "123 Parity Test Street"

	idempotencyHeader = "X-Idempotency-Key"

	statusCompleted = "COMPLETED"
	statusCancelled = "CANCELLED"
	statusFailed    = "FAILED"
)

var runCounter atomic.Uint64

var validProductFixtures = []productFixture{
	{ProductID: "PROD-001", ProductName: "Laptop", Quantity: 1, Price: "999.99", ExpectedFinalStatus: statusCompleted},
	{ProductID: "PROD-002", ProductName: "Headphones", Quantity: 1, Price: "149.99", ExpectedFinalStatus: statusCompleted},
	{ProductID: "PROD-003", ProductName: "Keyboard", Quantity: 1, Price: "79.99", ExpectedFinalStatus: statusCompleted},
	{ProductID: "PROD-004", ProductName: "Mouse", Quantity: 1, Price: "49.99", ExpectedFinalStatus: statusCompleted},
	{ProductID: "PROD-005", ProductName: "Monitor", Quantity: 1, Price: "299.99", ExpectedFinalStatus: statusCompleted},
	{ProductID: "PROD-006", ProductName: "Webcam", Quantity: 2, Price: "89.99", ExpectedFinalStatus: statusCompleted},
	{ProductID: "PROD-007", ProductName: "USB Hub", Quantity: 1, Price: "29.99", ExpectedFinalStatus: statusCompleted},
	{ProductID: "PROD-008", ProductName: "Mousepad", Quantity: 1, Price: "19.99", ExpectedFinalStatus: statusCompleted},
}

var paymentFailureFixture = productFixture{
	ProductID:           "PROD-PREMIUM-001",
	ProductName:         "Premium Item",
	Quantity:            1,
	Price:               "10000.00",
	ExpectedFinalStatus: statusCancelled,
	FailureReason:       "payment_failure",
}

var inventoryFailureFixtures = []productFixture{
	{ProductID: "PROD-LOW-001", ProductName: "Rare Item", Quantity: 100, Price: "25.00", ExpectedFinalStatus: statusCancelled, FailureReason: "inventory_failure"},
	{ProductID: "PROD-LOW-002", ProductName: "Limited Item", Quantity: 100, Price: "30.00", ExpectedFinalStatus: statusCancelled, FailureReason: "inventory_failure"},
}

type parityConfig struct {
	Pattern         string
	Stack           string
	BaseURL         string
	MaxPollAttempts int
	PollInterval    time.Duration
	HTTPTimeout     time.Duration
	Client          *http.Client
	Terminal        map[string]struct{}
}

type productFixture struct {
	ProductID           string
	ProductName         string
	Quantity            int
	Price               string
	ExpectedFinalStatus string
	FailureReason       string
}

type createResult struct {
	StatusCode int
	OrderID    string
	Payload    map[string]any
	Body       string
}

type pollResult struct {
	StatusCode    int
	FinalStatus   string
	PollAttempts  int
	Temporary404  bool
	LastBody      string
	LastOrderID   string
	LastHTTPCode  int
	ObservedReady bool
}

type scenarioResult struct {
	Scenario            string `json:"scenario"`
	Pattern             string `json:"pattern"`
	Stack               string `json:"stack"`
	BaseURL             string `json:"baseUrl"`
	ProductID           string `json:"productId,omitempty"`
	ExpectedFinalStatus string `json:"expectedFinalStatus,omitempty"`
	ObservedFinalStatus string `json:"observedFinalStatus,omitempty"`
	CreateStatusCode    int    `json:"createStatusCode,omitempty"`
	OrderID             string `json:"orderId,omitempty"`
	PollAttempts        int    `json:"pollAttempts,omitempty"`
	Temporary404Seen    bool   `json:"temporary404Seen,omitempty"`
	IdempotencyKey      string `json:"idempotencyKey,omitempty"`
	DuplicateOrderID    string `json:"duplicateOrderId,omitempty"`
	Observation         string `json:"observation,omitempty"`
	Pass                bool   `json:"pass"`
}

func TestHappyPathSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping parity smoke in short mode")
	}

	cfg := loadParityConfig(t)
	if err := checkHealth(cfg); err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	result, err := runOrderScenario(cfg, "happy_path", validProductFixtures[0])
	emitScenarioResult(t, result)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIdempotencyAndFailureScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping parity smoke in short mode")
	}

	cfg := loadParityConfig(t)

	t.Run("payment_failure", func(t *testing.T) {
		result, err := runOrderScenario(cfg, "payment_failure", paymentFailureFixture)
		emitScenarioResult(t, result)
		if err != nil {
			t.Fatal(err)
		}
	})

	for _, fixture := range inventoryFailureFixtures {
		fixture := fixture
		t.Run("inventory_failure_"+strings.ToLower(fixture.ProductID), func(t *testing.T) {
			result, err := runOrderScenario(cfg, "inventory_failure", fixture)
			emitScenarioResult(t, result)
			if err != nil {
				t.Fatal(err)
			}
		})
	}

	t.Run("duplicate_submission", func(t *testing.T) {
		result, err := runDuplicateSubmissionScenario(cfg, validProductFixtures[1])
		emitScenarioResult(t, result)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("temporary_404_polling_tolerance", func(t *testing.T) {
		result, err := runTemporary404ToleranceScenario()
		emitScenarioResult(t, result)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func loadParityConfig(t *testing.T) parityConfig {
	t.Helper()

	pattern := strings.ToLower(strings.TrimSpace(os.Getenv("PARITY_PATTERN")))
	if pattern == "" {
		pattern = "choreography"
	}
	if pattern != "choreography" && pattern != "orchestration" {
		t.Fatalf("unsupported PARITY_PATTERN %q", pattern)
	}

	stack := strings.TrimSpace(os.Getenv("PARITY_STACK"))
	if stack == "" {
		stack = "baseline"
	}

	baseURL := strings.TrimSpace(os.Getenv("PARITY_BASE_URL"))
	if baseURL == "" {
		if pattern == "orchestration" {
			baseURL = "http://localhost:8085"
		} else {
			baseURL = "http://localhost:8081"
		}
	}

	maxPollAttempts := getenvInt("PARITY_MAX_POLL_ATTEMPTS", defaultPollAttempts)
	pollInterval := time.Duration(getenvInt("PARITY_POLL_INTERVAL_MS", int(defaultPollInterval/time.Millisecond))) * time.Millisecond
	httpTimeout := time.Duration(getenvInt("PARITY_HTTP_TIMEOUT_MS", int(defaultHTTPTimeout/time.Millisecond))) * time.Millisecond

	return parityConfig{
		Pattern:         pattern,
		Stack:           stack,
		BaseURL:         strings.TrimRight(baseURL, "/"),
		MaxPollAttempts: maxPollAttempts,
		PollInterval:    pollInterval,
		HTTPTimeout:     httpTimeout,
		Client:          &http.Client{Timeout: httpTimeout},
		Terminal: map[string]struct{}{
			statusCompleted: {},
			statusCancelled: {},
			statusFailed:    {},
		},
	}
}

func checkHealth(cfg parityConfig) error {
	req, err := http.NewRequest(http.MethodGet, cfg.BaseURL+healthPath, nil)
	if err != nil {
		return err
	}
	resp, err := cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("health status = %d, body = %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func runOrderScenario(cfg parityConfig, scenario string, fixture productFixture) (scenarioResult, error) {
	create, err := submitOrder(cfg, fixture, "")
	if err != nil {
		return scenarioResult{Scenario: scenario, Pattern: cfg.Pattern, Stack: cfg.Stack, BaseURL: cfg.BaseURL, ProductID: fixture.ProductID, ExpectedFinalStatus: fixture.ExpectedFinalStatus, Pass: false}, err
	}

	poll, err := pollOrderUntilTerminal(cfg, create.OrderID)
	result := scenarioResult{
		Scenario:            scenario,
		Pattern:             cfg.Pattern,
		Stack:               cfg.Stack,
		BaseURL:             cfg.BaseURL,
		ProductID:           fixture.ProductID,
		ExpectedFinalStatus: fixture.ExpectedFinalStatus,
		ObservedFinalStatus: poll.FinalStatus,
		CreateStatusCode:    create.StatusCode,
		OrderID:             create.OrderID,
		PollAttempts:        poll.PollAttempts,
		Temporary404Seen:    poll.Temporary404,
		Pass:                err == nil && poll.FinalStatus == fixture.ExpectedFinalStatus,
	}
	if err != nil {
		return result, fmt.Errorf("%s scenario failed for %s: %w", scenario, fixture.ProductID, err)
	}
	if poll.FinalStatus != fixture.ExpectedFinalStatus {
		result.Pass = false
		return result, fmt.Errorf("%s scenario final status for %s = %q, want %q", scenario, fixture.ProductID, poll.FinalStatus, fixture.ExpectedFinalStatus)
	}
	return result, nil
}

func runDuplicateSubmissionScenario(cfg parityConfig, fixture productFixture) (scenarioResult, error) {
	idempotencyKey := fmt.Sprintf("IDEM-%s-%d", strings.ToUpper(cfg.Pattern[:1]), runCounter.Add(1))

	firstCreate, err := submitOrder(cfg, fixture, idempotencyKey)
	if err != nil {
		return scenarioResult{Scenario: "duplicate_submission", Pattern: cfg.Pattern, Stack: cfg.Stack, BaseURL: cfg.BaseURL, ProductID: fixture.ProductID, IdempotencyKey: idempotencyKey, Pass: false}, err
	}
	firstPoll, err := pollOrderUntilTerminal(cfg, firstCreate.OrderID)
	if err != nil {
		return scenarioResult{Scenario: "duplicate_submission", Pattern: cfg.Pattern, Stack: cfg.Stack, BaseURL: cfg.BaseURL, ProductID: fixture.ProductID, CreateStatusCode: firstCreate.StatusCode, OrderID: firstCreate.OrderID, IdempotencyKey: idempotencyKey, PollAttempts: firstPoll.PollAttempts, Temporary404Seen: firstPoll.Temporary404, Pass: false}, err
	}
	if firstPoll.FinalStatus != fixture.ExpectedFinalStatus {
		return scenarioResult{Scenario: "duplicate_submission", Pattern: cfg.Pattern, Stack: cfg.Stack, BaseURL: cfg.BaseURL, ProductID: fixture.ProductID, CreateStatusCode: firstCreate.StatusCode, OrderID: firstCreate.OrderID, IdempotencyKey: idempotencyKey, ObservedFinalStatus: firstPoll.FinalStatus, Pass: false}, fmt.Errorf("first duplicate probe order final status = %q, want %q", firstPoll.FinalStatus, fixture.ExpectedFinalStatus)
	}

	duplicateCreate, err := submitOrder(cfg, fixture, idempotencyKey)
	if err != nil {
		return scenarioResult{Scenario: "duplicate_submission", Pattern: cfg.Pattern, Stack: cfg.Stack, BaseURL: cfg.BaseURL, ProductID: fixture.ProductID, CreateStatusCode: firstCreate.StatusCode, OrderID: firstCreate.OrderID, IdempotencyKey: idempotencyKey, Pass: false}, err
	}

	observation := "idempotent_duplicate_reused_order_id"
	pass := true
	if duplicateCreate.OrderID != firstCreate.OrderID {
		observation = "http_idempotency_not_observed"
		duplicatePoll, pollErr := pollOrderUntilTerminal(cfg, duplicateCreate.OrderID)
		if pollErr != nil {
			return scenarioResult{
				Scenario:            "duplicate_submission",
				Pattern:             cfg.Pattern,
				Stack:               cfg.Stack,
				BaseURL:             cfg.BaseURL,
				ProductID:           fixture.ProductID,
				ExpectedFinalStatus: fixture.ExpectedFinalStatus,
				ObservedFinalStatus: duplicatePoll.FinalStatus,
				CreateStatusCode:    firstCreate.StatusCode,
				OrderID:             firstCreate.OrderID,
				DuplicateOrderID:    duplicateCreate.OrderID,
				IdempotencyKey:      idempotencyKey,
				Observation:         observation,
				PollAttempts:        firstPoll.PollAttempts + duplicatePoll.PollAttempts,
				Temporary404Seen:    firstPoll.Temporary404 || duplicatePoll.Temporary404,
				Pass:                false,
			}, pollErr
		}
		if duplicatePoll.FinalStatus != fixture.ExpectedFinalStatus {
			pass = false
			return scenarioResult{
				Scenario:            "duplicate_submission",
				Pattern:             cfg.Pattern,
				Stack:               cfg.Stack,
				BaseURL:             cfg.BaseURL,
				ProductID:           fixture.ProductID,
				ExpectedFinalStatus: fixture.ExpectedFinalStatus,
				ObservedFinalStatus: duplicatePoll.FinalStatus,
				CreateStatusCode:    firstCreate.StatusCode,
				OrderID:             firstCreate.OrderID,
				DuplicateOrderID:    duplicateCreate.OrderID,
				IdempotencyKey:      idempotencyKey,
				Observation:         observation,
				PollAttempts:        firstPoll.PollAttempts + duplicatePoll.PollAttempts,
				Temporary404Seen:    firstPoll.Temporary404 || duplicatePoll.Temporary404,
				Pass:                false,
			}, fmt.Errorf("duplicate probe order final status = %q, want %q", duplicatePoll.FinalStatus, fixture.ExpectedFinalStatus)
		}
		return scenarioResult{
			Scenario:            "duplicate_submission",
			Pattern:             cfg.Pattern,
			Stack:               cfg.Stack,
			BaseURL:             cfg.BaseURL,
			ProductID:           fixture.ProductID,
			ExpectedFinalStatus: fixture.ExpectedFinalStatus,
			ObservedFinalStatus: firstPoll.FinalStatus,
			CreateStatusCode:    firstCreate.StatusCode,
			OrderID:             firstCreate.OrderID,
			DuplicateOrderID:    duplicateCreate.OrderID,
			IdempotencyKey:      idempotencyKey,
			Observation:         observation,
			PollAttempts:        firstPoll.PollAttempts + duplicatePoll.PollAttempts,
			Temporary404Seen:    firstPoll.Temporary404 || duplicatePoll.Temporary404,
			Pass:                pass,
		}, nil
	}

	return scenarioResult{
		Scenario:            "duplicate_submission",
		Pattern:             cfg.Pattern,
		Stack:               cfg.Stack,
		BaseURL:             cfg.BaseURL,
		ProductID:           fixture.ProductID,
		ExpectedFinalStatus: fixture.ExpectedFinalStatus,
		ObservedFinalStatus: firstPoll.FinalStatus,
		CreateStatusCode:    firstCreate.StatusCode,
		OrderID:             firstCreate.OrderID,
		DuplicateOrderID:    duplicateCreate.OrderID,
		IdempotencyKey:      idempotencyKey,
		Observation:         observation,
		PollAttempts:        firstPoll.PollAttempts,
		Temporary404Seen:    firstPoll.Temporary404,
		Pass:                true,
	}, nil
}

func runTemporary404ToleranceScenario() (scenarioResult, error) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != ordersPath+"/test-order" {
			http.NotFound(w, r)
			return
		}
		switch requests.Add(1) {
		case 1, 2:
			http.NotFound(w, r)
		case 3:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"orderId":"test-order","status":"PENDING"}`)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"orderId":"test-order","status":"COMPLETED"}`)
		}
	}))
	defer server.Close()

	cfg := parityConfig{
		Pattern:         "temporary-404-probe",
		Stack:           "synthetic",
		BaseURL:         server.URL,
		MaxPollAttempts: 6,
		PollInterval:    10 * time.Millisecond,
		HTTPTimeout:     defaultHTTPTimeout,
		Client:          server.Client(),
		Terminal: map[string]struct{}{
			statusCompleted: {},
			statusCancelled: {},
			statusFailed:    {},
		},
	}

	poll, err := pollOrderUntilTerminal(cfg, "test-order")
	result := scenarioResult{
		Scenario:            "temporary_404_polling_tolerance",
		Pattern:             cfg.Pattern,
		Stack:               cfg.Stack,
		BaseURL:             cfg.BaseURL,
		ExpectedFinalStatus: statusCompleted,
		ObservedFinalStatus: poll.FinalStatus,
		OrderID:             "test-order",
		PollAttempts:        poll.PollAttempts,
		Temporary404Seen:    poll.Temporary404,
		Observation:         "poller_accepts_404_until_terminal_state_is_visible",
		Pass:                err == nil && poll.Temporary404 && poll.FinalStatus == statusCompleted,
	}
	if err != nil {
		return result, err
	}
	if !poll.Temporary404 {
		result.Pass = false
		return result, fmt.Errorf("expected temporary 404s to be tolerated before terminal status")
	}
	if poll.FinalStatus != statusCompleted {
		result.Pass = false
		return result, fmt.Errorf("temporary 404 probe final status = %q, want %q", poll.FinalStatus, statusCompleted)
	}
	return result, nil
}

func submitOrder(cfg parityConfig, fixture productFixture, idempotencyKey string) (createResult, error) {
	payload, err := marshalCreateOrder(cfg.Pattern, uniqueCustomerID(cfg.Pattern), fixture)
	if err != nil {
		return createResult{}, err
	}

	req, err := http.NewRequest(http.MethodPost, cfg.BaseURL+ordersPath, bytes.NewReader(payload))
	if err != nil {
		return createResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set(idempotencyHeader, idempotencyKey)
	}

	resp, err := cfg.Client.Do(req)
	if err != nil {
		return createResult{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return createResult{}, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return createResult{}, fmt.Errorf("create order status = %d, body = %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payloadMap map[string]any
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &payloadMap); err != nil {
			return createResult{}, fmt.Errorf("decode create response: %w (body=%s)", err, strings.TrimSpace(string(body)))
		}
	}
	orderID := stringField(payloadMap, "orderId")
	if orderID == "" {
		orderID = stringField(payloadMap, "id")
	}
	if orderID == "" {
		return createResult{}, fmt.Errorf("create response missing orderId/id field: %s", strings.TrimSpace(string(body)))
	}

	return createResult{StatusCode: resp.StatusCode, OrderID: orderID, Payload: payloadMap, Body: strings.TrimSpace(string(body))}, nil
}

func pollOrderUntilTerminal(cfg parityConfig, orderID string) (pollResult, error) {
	result := pollResult{LastOrderID: orderID}
	for attempt := 1; attempt <= cfg.MaxPollAttempts; attempt++ {
		time.Sleep(cfg.PollInterval)

		req, err := http.NewRequest(http.MethodGet, cfg.BaseURL+ordersPath+"/"+orderID, nil)
		if err != nil {
			return result, err
		}
		resp, err := cfg.Client.Do(req)
		if err != nil {
			return result, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return result, readErr
		}

		result.PollAttempts = attempt
		result.LastHTTPCode = resp.StatusCode
		result.LastBody = strings.TrimSpace(string(body))

		switch resp.StatusCode {
		case http.StatusOK:
			result.ObservedReady = true
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				return result, fmt.Errorf("decode poll response: %w (body=%s)", err, strings.TrimSpace(string(body)))
			}
			result.FinalStatus = stringField(payload, "status")
			if result.FinalStatus == "" {
				return result, fmt.Errorf("poll response missing status field: %s", strings.TrimSpace(string(body)))
			}
			if _, ok := cfg.Terminal[result.FinalStatus]; ok {
				result.StatusCode = resp.StatusCode
				return result, nil
			}
		case http.StatusNotFound:
			result.Temporary404 = true
		default:
			return result, fmt.Errorf("poll status = %d, body = %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
	}
	return result, fmt.Errorf("order %s did not reach terminal status after %d polls (last_http=%d last_status=%q body=%s)", orderID, cfg.MaxPollAttempts, result.LastHTTPCode, result.FinalStatus, result.LastBody)
}

func marshalCreateOrder(pattern, customerID string, fixture productFixture) ([]byte, error) {
	item := dto.OrderItemRequest{
		ProductID:   fixture.ProductID,
		ProductName: fixture.ProductName,
		Quantity:    fixture.Quantity,
		Price:       json.Number(fixture.Price),
	}
	if pattern == "orchestration" {
		total, err := totalAmount(fixture)
		if err != nil {
			return nil, err
		}
		return json.Marshal(dto.OrchestrationCreateOrderRequest{
			CustomerID:      customerID,
			ShippingAddress: defaultShippingStreet,
			TotalAmount:     total,
			Items:           []dto.OrderItemRequest{item},
		})
	}
	return json.Marshal(dto.ChoreographyCreateOrderRequest{
		CustomerID:      customerID,
		ShippingAddress: defaultShippingStreet,
		Items:           []dto.OrderItemRequest{item},
	})
}

func totalAmount(fixture productFixture) (json.Number, error) {
	price, err := strconv.ParseFloat(fixture.Price, 64)
	if err != nil {
		return "", fmt.Errorf("parse fixture price %q: %w", fixture.Price, err)
	}
	total := price * float64(fixture.Quantity)
	return json.Number(strconv.FormatFloat(total, 'f', 2, 64)), nil
}

func uniqueCustomerID(pattern string) string {
	value := runCounter.Add(1)
	return fmt.Sprintf("PARITY-%s-%d", strings.ToUpper(pattern), value)
}

func emitScenarioResult(t *testing.T, result scenarioResult) {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal scenario result: %v", err)
	}
	t.Logf("PARITY_RESULT %s", encoded)
}

func stringField(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, ok := payload[key]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
