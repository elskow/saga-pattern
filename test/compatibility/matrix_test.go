package compatibility

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type compatibilityMatrix struct {
	GeneratedFrom  []string `json:"generatedFrom"`
	Classification struct {
		FixedContracts     []string `json:"fixedContracts"`
		InternalOnly       []string `json:"internalOnly"`
		AllowedAsymmetries []string `json:"allowedAsymmetries"`
		KnownConflicts     []string `json:"knownConflicts"`
	} `json:"classification"`
	Ports struct {
		LocalSurface struct {
			Choreography   map[string]int `json:"choreography"`
			Orchestration  map[string]int `json:"orchestration"`
			HealthPath     string         `json:"healthPath"`
			PrometheusPath string         `json:"prometheusPath"`
		} `json:"localSurface"`
		VMBenchmarkSurface struct {
			Choreography struct {
				OrderReplicas []int `json:"orderReplicas"`
				Payment       int   `json:"payment"`
				Inventory     int   `json:"inventory"`
				Shipping      int   `json:"shipping"`
			} `json:"choreography"`
			Orchestration struct {
				OrderReplicas []int `json:"orderReplicas"`
				Payment       int   `json:"payment"`
				Inventory     int   `json:"inventory"`
				Shipping      int   `json:"shipping"`
			} `json:"orchestration"`
		} `json:"vmBenchmarkSurface"`
	} `json:"ports"`
	HTTP struct {
		BaseRoute      string   `json:"baseRoute"`
		HealthPath     string   `json:"healthPath"`
		PrometheusPath string   `json:"prometheusPath"`
		CommonRoutes   []string `json:"commonRoutes"`
		CreateOrder    struct {
			AcceptedStatusCodes []int `json:"acceptedStatusCodes"`
			Variants            map[string]struct {
				RequiredRequestFields  []string `json:"requiredRequestFields"`
				RequiredItemFields     []string `json:"requiredItemFields"`
				ForbiddenRequestFields []string `json:"forbiddenRequestFields"`
				ResponseStatusCode     int      `json:"responseStatusCode"`
				RequiredResponseFields []string `json:"requiredResponseFields"`
				FixedResponseStatus    string   `json:"fixedResponseStatus"`
			} `json:"variants"`
		} `json:"createOrder"`
		Polling struct {
			AcceptedStatusCodes []int    `json:"acceptedStatusCodes"`
			TerminalStatuses    []string `json:"terminalStatuses"`
		} `json:"polling"`
		InternalStatuses map[string][]string `json:"internalStatuses"`
	} `json:"http"`
	Kafka struct {
		Topics                  map[string]string `json:"topics"`
		ChoreographyEventTypes  []string          `json:"choreographyEventTypes"`
		OrchestrationReplyTypes []string          `json:"orchestrationReplyTypes"`
	} `json:"kafka"`
	Metrics struct {
		RequiredMetricNames []string `json:"requiredMetricNames"`
		LabelKeys           []string `json:"labelKeys"`
		LabelAssumptions    []string `json:"labelAssumptions"`
		QuerySurfaces       []string `json:"querySurfaces"`
	} `json:"metrics"`
	Jenkins struct {
		Parameters  []string `json:"parameters"`
		Profiles    []string `json:"profiles"`
		Simulations []string `json:"simulations"`
	} `json:"jenkins"`
	Gatling struct {
		PatternPropertyValues       []string       `json:"patternPropertyValues"`
		AcceptedCreateOrderStatuses []int          `json:"acceptedCreateOrderStatuses"`
		AcceptedPollStatuses        []int          `json:"acceptedPollStatuses"`
		TerminalStatuses            []string       `json:"terminalStatuses"`
		DefaultBasePorts            map[string]int `json:"defaultBasePorts"`
		NamedSimulations            []string       `json:"namedSimulations"`
	} `json:"gatling"`
	Products struct {
		Standard []struct {
			ProductID       string `json:"productId"`
			ExpectedOutcome string `json:"expectedOutcome"`
		} `json:"standard"`
		LowStock struct {
			ProductID       string `json:"productId"`
			ExpectedOutcome string `json:"expectedOutcome"`
			FailureReason   string `json:"failureReason"`
		} `json:"lowStock"`
		HighValue struct {
			ProductID       string `json:"productId"`
			ExpectedOutcome string `json:"expectedOutcome"`
			FailureReason   string `json:"failureReason"`
		} `json:"highValue"`
	} `json:"products"`
}

func TestCompatibilityMatrixComplete(t *testing.T) {
	matrix := loadMatrix(t, fixturePath(t))
	if err := validateMatrix(matrix); err != nil {
		t.Fatalf("compatibility matrix incomplete: %v", err)
	}
}

func TestGatlingSurfaceFixtures(t *testing.T) {
	matrix := loadMatrix(t, fixturePath(t))

	if !slices.Equal(matrix.Gatling.AcceptedCreateOrderStatuses, []int{200, 201, 202}) {
		t.Fatalf("accepted create-order statuses mismatch: %v", matrix.Gatling.AcceptedCreateOrderStatuses)
	}
	if !slices.Equal(matrix.Gatling.AcceptedPollStatuses, []int{200, 404}) {
		t.Fatalf("accepted poll statuses mismatch: %v", matrix.Gatling.AcceptedPollStatuses)
	}
	if !slices.Equal(matrix.Gatling.TerminalStatuses, []string{"COMPLETED", "CANCELLED", "FAILED"}) {
		t.Fatalf("terminal statuses mismatch: %v", matrix.Gatling.TerminalStatuses)
	}
	if matrix.Gatling.DefaultBasePorts["choreography"] != 8081 || matrix.Gatling.DefaultBasePorts["orchestration"] != 8085 {
		t.Fatalf("unexpected Gatling base ports: %#v", matrix.Gatling.DefaultBasePorts)
	}

	choreo := matrix.HTTP.CreateOrder.Variants["choreography"]
	orch := matrix.HTTP.CreateOrder.Variants["orchestration"]
	if slices.Contains(choreo.RequiredRequestFields, "totalAmount") {
		t.Fatal("choreography request must not require totalAmount")
	}
	if !slices.Contains(choreo.ForbiddenRequestFields, "totalAmount") {
		t.Fatal("choreography request must explicitly forbid totalAmount in frozen fixtures")
	}
	if !slices.Contains(orch.RequiredRequestFields, "totalAmount") {
		t.Fatal("orchestration request must require totalAmount")
	}
	if choreo.ResponseStatusCode != 201 {
		t.Fatalf("choreography create-order status = %d, want 201", choreo.ResponseStatusCode)
	}
	if orch.ResponseStatusCode != 202 || orch.FixedResponseStatus != "SAGA_STARTED" {
		t.Fatalf("orchestration create-order response mismatch: status=%d bodyStatus=%q", orch.ResponseStatusCode, orch.FixedResponseStatus)
	}

	if len(matrix.Products.Standard) < 4 {
		t.Fatalf("expected at least four standard product fixtures, got %d", len(matrix.Products.Standard))
	}
	if matrix.Products.HighValue.ProductID != "PROD-PREMIUM-001" || matrix.Products.HighValue.ExpectedOutcome != "CANCELLED" || matrix.Products.HighValue.FailureReason != "payment_failure" {
		t.Fatalf("unexpected high-value product fixture: %#v", matrix.Products.HighValue)
	}
	if matrix.Products.LowStock.ProductID != "PROD-LOW-001" || matrix.Products.LowStock.ExpectedOutcome != "CANCELLED" || matrix.Products.LowStock.FailureReason != "inventory_failure" {
		t.Fatalf("unexpected low-stock product fixture: %#v", matrix.Products.LowStock)
	}

	if matrix.Ports.LocalSurface.Choreography["order"] != 8081 || matrix.Ports.LocalSurface.Orchestration["order"] != 8085 {
		t.Fatalf("unexpected local order ports: choreography=%d orchestration=%d", matrix.Ports.LocalSurface.Choreography["order"], matrix.Ports.LocalSurface.Orchestration["order"])
	}
	if !slices.Equal(matrix.Ports.VMBenchmarkSurface.Choreography.OrderReplicas, []int{8081, 8082, 8083, 8084}) {
		t.Fatalf("unexpected VM choreography replica ports: %v", matrix.Ports.VMBenchmarkSurface.Choreography.OrderReplicas)
	}
	if !slices.Equal(matrix.Ports.VMBenchmarkSurface.Orchestration.OrderReplicas, []int{8085, 8086, 8087, 8088}) {
		t.Fatalf("unexpected VM orchestration replica ports: %v", matrix.Ports.VMBenchmarkSurface.Orchestration.OrderReplicas)
	}
	if matrix.Ports.VMBenchmarkSurface.Choreography.Payment != 8091 || matrix.Ports.VMBenchmarkSurface.Orchestration.Shipping != 8093 {
		t.Fatalf("unexpected VM benchmark service ports: %+v %+v", matrix.Ports.VMBenchmarkSurface.Choreography, matrix.Ports.VMBenchmarkSurface.Orchestration)
	}
}

func TestRejectsMissingMetricOrTopicFixture(t *testing.T) {
	matrix := loadMatrix(t, fixturePath(t))

	missingMetric := matrix
	missingMetric.Metrics.RequiredMetricNames = withoutString(missingMetric.Metrics.RequiredMetricNames, "saga_orders_created_total")
	if err := validateMatrix(missingMetric); err == nil || !strings.Contains(err.Error(), "saga_orders_created_total") {
		t.Fatalf("expected missing metric validation error, got %v", err)
	}

	missingTopic := matrix
	delete(missingTopic.Kafka.Topics, "paymentReplies")
	if err := validateMatrix(missingTopic); err == nil || !strings.Contains(err.Error(), "paymentReplies") {
		t.Fatalf("expected missing topic validation error, got %v", err)
	}
}

func TestDocsMatchCompatibilityMatrix(t *testing.T) {
	matrix := loadMatrix(t, fixturePath(t))

	type docCheck struct {
		path     string
		required []string
		forbid   []string
	}

	orchestrationReplicaPorts := joinInts(matrix.Ports.VMBenchmarkSurface.Orchestration.OrderReplicas)
	participantPorts := joinInts([]int{
		matrix.Ports.VMBenchmarkSurface.Orchestration.Payment,
		matrix.Ports.VMBenchmarkSurface.Orchestration.Inventory,
		matrix.Ports.VMBenchmarkSurface.Orchestration.Shipping,
	})

	checks := []docCheck{
		{
			path: "../../README.md",
			required: []string{
				"default runtime is now Go",
				"make build",
				"make test",
				"make up-choreography",
				"make up-orchestration",
				"make verify-thesis-surface",
				"make verify-jenkins-benchmark",
								fmt.Sprintf("benchmark entrypoint on `%d`", matrix.Ports.VMBenchmarkSurface.Orchestration.OrderReplicas[0]),
				fmt.Sprintf("`%s`", orchestrationReplicaPorts),
				fmt.Sprintf("`%s`", participantPorts),
				matrix.HTTP.CommonRoutes[0],
				matrix.HTTP.CommonRoutes[1],
				"status=SAGA_STARTED",
				"saga_orders_created_total",
				"saga_framework_step_duration_seconds",
			},
			forbid: []string{
				"Eventuate Tram",
				"Spring State Machine",
			},
		},
		{
			path: "../../docs/README.md",
			required: []string{
				"default runtime is Go",
				"make build",
				"make test",
				"make verify-thesis-surface",
				"make verify-jenkins-benchmark",
								fmt.Sprintf("ports `%d` to `%d`", matrix.Ports.LocalSurface.Choreography["order"], matrix.Ports.LocalSurface.Choreography["shipping"]),
				fmt.Sprintf("entrypoint on `%d`", matrix.Ports.VMBenchmarkSurface.Orchestration.OrderReplicas[0]),
				fmt.Sprintf("`%s`", participantPorts),
				"Kafka is the transport for both variants.",
			},
			forbid: []string{
				"Eventuate Tram",
				"Spring State Machine",
			},
		},
		{
			path: "../../docs/GETTING-STARTED.md",
			required: []string{
				"Go implementation as the default runtime path",
				"make build",
				"make test",
				"make test-parity",
				"make smoke-choreography-go",
				"make smoke-orchestration-go",
								fmt.Sprintf("`%s`", orchestrationReplicaPorts),
				fmt.Sprintf("`%s`", participantPorts),
				"Kafka is the transport for both saga variants.",
			},
			forbid: []string{
				"Spring State Machine",
				"mvn clean package -DskipTests",
			},
		},
		{
			path: "../../docs/TESTING.md",
			required: append([]string{
				"make verify-thesis-surface",
				"make verify-jenkins-benchmark",
				"go test ./test/compatibility/... -run TestDocsMatchCompatibilityMatrix",
								fmt.Sprintf("| Orchestration | %d | %s | %s |", matrix.Ports.VMBenchmarkSurface.Orchestration.OrderReplicas[0], orchestrationReplicaPorts, participantPorts),
			}, matrix.Jenkins.Parameters...),
			forbid: []string{},
		},
		{
			path: "../../docs/THESIS.md",
			required: []string{
				"same workload, the same metrics, and the same observability surface",
				"active choreography path is Go over Kafka",
				"active orchestration path is Go with an order service orchestrator over Kafka and Postgres",
								fmt.Sprintf("`%s`", orchestrationReplicaPorts),
				fmt.Sprintf("`%s`", participantPorts),
								"saga_total_duration_seconds",
				"saga_framework_duration_seconds",
			},
			forbid: []string{
				"Eventuate Tram",
				"Spring State Machine",
			},
		},
	}

	for _, check := range checks {
		content := readRelativeFile(t, check.path)
		for _, required := range check.required {
			if !strings.Contains(content, required) {
				t.Fatalf("%s missing required text %q", check.path, required)
			}
		}
		for _, forbidden := range check.forbid {
			if strings.Contains(content, forbidden) {
				t.Fatalf("%s contains stale text %q", check.path, forbidden)
			}
		}
	}

	for _, metric := range []string{
		"saga_orders_created_total",
		"saga_orders_completed_total",
		"saga_orders_failed_total",
		"saga_order_processing_time_seconds",
		"saga_total_duration_seconds",
		"saga_framework_duration_seconds",
		"saga_framework_step_duration_seconds",
	} {
		if !slices.Contains(matrix.Metrics.RequiredMetricNames, metric) {
			t.Fatalf("compatibility matrix missing metric expected by docs test: %s", metric)
		}
	}
}

func loadMatrix(t *testing.T, path string) compatibilityMatrix {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	var matrix compatibilityMatrix
	if err := json.Unmarshal(data, &matrix); err != nil {
		t.Fatalf("decode fixture %s: %v", path, err)
	}
	return matrix
}

func fixturePath(t *testing.T) string {
	t.Helper()
	return filepath.Join("fixtures", "compatibility-matrix.json")
}

func validateMatrix(m compatibilityMatrix) error {
	for _, section := range []struct {
		name   string
		values []string
	}{
		{name: "generatedFrom", values: m.GeneratedFrom},
		{name: "classification.fixedContracts", values: m.Classification.FixedContracts},
		{name: "classification.internalOnly", values: m.Classification.InternalOnly},
		{name: "classification.allowedAsymmetries", values: m.Classification.AllowedAsymmetries},
		{name: "classification.knownConflicts", values: m.Classification.KnownConflicts},
		{name: "metrics.requiredMetricNames", values: m.Metrics.RequiredMetricNames},
		{name: "metrics.labelKeys", values: m.Metrics.LabelKeys},
		{name: "metrics.labelAssumptions", values: m.Metrics.LabelAssumptions},
		{name: "metrics.querySurfaces", values: m.Metrics.QuerySurfaces},
		{name: "jenkins.parameters", values: m.Jenkins.Parameters},
		{name: "jenkins.profiles", values: m.Jenkins.Profiles},
		{name: "jenkins.simulations", values: m.Jenkins.Simulations},
		{name: "gatling.patternPropertyValues", values: m.Gatling.PatternPropertyValues},
		{name: "gatling.namedSimulations", values: m.Gatling.NamedSimulations},
		{name: "http.commonRoutes", values: m.HTTP.CommonRoutes},
		{name: "http.polling.terminalStatuses", values: m.HTTP.Polling.TerminalStatuses},
		{name: "kafka.choreographyEventTypes", values: m.Kafka.ChoreographyEventTypes},
		{name: "kafka.orchestrationReplyTypes", values: m.Kafka.OrchestrationReplyTypes},
	} {
		if len(section.values) == 0 {
			return fmt.Errorf("missing required section %s", section.name)
		}
	}

	if m.HTTP.BaseRoute != "/api/orders" || m.HTTP.HealthPath != "/actuator/health" || m.HTTP.PrometheusPath != "/actuator/prometheus" {
		return fmt.Errorf("unexpected HTTP compatibility paths: base=%q health=%q metrics=%q", m.HTTP.BaseRoute, m.HTTP.HealthPath, m.HTTP.PrometheusPath)
	}

	for variant, expected := range map[string]int{"choreography": 201, "orchestration": 202} {
		v, ok := m.HTTP.CreateOrder.Variants[variant]
		if !ok {
			return fmt.Errorf("missing http.createOrder variant %q", variant)
		}
		if len(v.RequiredRequestFields) == 0 || len(v.RequiredItemFields) == 0 || len(v.RequiredResponseFields) == 0 {
			return fmt.Errorf("incomplete create-order fixture for %s", variant)
		}
		if v.ResponseStatusCode != expected {
			return fmt.Errorf("unexpected create-order response code for %s: %d", variant, v.ResponseStatusCode)
		}
	}

	if slices.Contains(m.HTTP.CreateOrder.Variants["choreography"].RequiredRequestFields, "totalAmount") {
		return errors.New("choreography create-order must not require totalAmount")
	}
	if !slices.Contains(m.HTTP.CreateOrder.Variants["choreography"].ForbiddenRequestFields, "totalAmount") {
		return errors.New("choreography create-order must explicitly preserve the totalAmount asymmetry")
	}
	if !slices.Contains(m.HTTP.CreateOrder.Variants["orchestration"].RequiredRequestFields, "totalAmount") {
		return errors.New("orchestration create-order must require totalAmount")
	}
	if m.HTTP.CreateOrder.Variants["orchestration"].FixedResponseStatus != "SAGA_STARTED" {
		return errors.New("orchestration fixed response status must stay SAGA_STARTED")
	}

	for key, expected := range map[string]string{
		"orderEvents":       "order-events",
		"paymentEvents":     "payment-events",
		"inventoryEvents":   "inventory-events",
		"shippingEvents":    "shipping-events",
		"paymentCommands":   "orchestration.payment.commands",
		"inventoryCommands": "orchestration.inventory.commands",
		"shippingCommands":  "orchestration.shipping.commands",
		"paymentReplies":    "orchestration.payment.replies",
		"inventoryReplies":  "orchestration.inventory.replies",
		"shippingReplies":   "orchestration.shipping.replies",
	} {
		if got := m.Kafka.Topics[key]; got != expected {
			return fmt.Errorf("topic fixture %s = %q, want %q", key, got, expected)
		}
	}

	for _, metric := range []string{
		"saga_orders_created_total",
		"saga_orders_completed_total",
		"saga_orders_failed_total",
		"saga_total_duration_seconds",
		"saga_framework_duration_seconds",
		"saga_compensations_total",
	} {
		if !slices.Contains(m.Metrics.RequiredMetricNames, metric) {
			return fmt.Errorf("required metric fixture missing %s", metric)
		}
	}

	for _, label := range []string{"service", "pattern", "direction", "type", "outcome", "step"} {
		if !slices.Contains(m.Metrics.LabelKeys, label) {
			return fmt.Errorf("required metric label fixture missing %s", label)
		}
	}

	for _, parameter := range []string{"PROFILE", "SIMULATION", "RUN_CHOREOGRAPHY", "RUN_ORCHESTRATION", "SCALE_FACTOR"} {
		if !slices.Contains(m.Jenkins.Parameters, parameter) {
			return fmt.Errorf("required Jenkins parameter fixture missing %s", parameter)
		}
	}

	for _, simulation := range []string{"HappyPathSimulation", "SustainedMixedSimulation", "IdempotencySimulation"} {
		if !slices.Contains(m.Gatling.NamedSimulations, simulation) {
			return fmt.Errorf("required Gatling simulation fixture missing %s", simulation)
		}
	}

	if !slices.Equal(m.Gatling.AcceptedCreateOrderStatuses, []int{200, 201, 202}) {
		return fmt.Errorf("accepted create-order statuses changed: %v", m.Gatling.AcceptedCreateOrderStatuses)
	}
	if !slices.Equal(m.Gatling.AcceptedPollStatuses, []int{200, 404}) {
		return fmt.Errorf("accepted poll statuses changed: %v", m.Gatling.AcceptedPollStatuses)
	}
	if !slices.Equal(m.Gatling.TerminalStatuses, []string{"COMPLETED", "CANCELLED", "FAILED"}) {
		return fmt.Errorf("terminal statuses changed: %v", m.Gatling.TerminalStatuses)
	}

	if len(m.Products.Standard) == 0 || m.Products.HighValue.ProductID == "" || m.Products.LowStock.ProductID == "" {
		return errors.New("product fixtures are incomplete")
	}
	if m.Products.HighValue.ProductID != "PROD-PREMIUM-001" || m.Products.LowStock.ProductID != "PROD-LOW-001" {
		return fmt.Errorf("unexpected failure product fixtures: highValue=%q lowStock=%q", m.Products.HighValue.ProductID, m.Products.LowStock.ProductID)
	}

	if m.Ports.LocalSurface.HealthPath != "/actuator/health" || m.Ports.LocalSurface.PrometheusPath != "/actuator/prometheus" {
		return fmt.Errorf("local surface paths changed: health=%q prometheus=%q", m.Ports.LocalSurface.HealthPath, m.Ports.LocalSurface.PrometheusPath)
	}
	if !slices.Equal(m.Ports.VMBenchmarkSurface.Choreography.OrderReplicas, []int{8081, 8082, 8083, 8084}) {
		return fmt.Errorf("unexpected choreography VM replica ports: %v", m.Ports.VMBenchmarkSurface.Choreography.OrderReplicas)
	}
	if !slices.Equal(m.Ports.VMBenchmarkSurface.Orchestration.OrderReplicas, []int{8085, 8086, 8087, 8088}) {
		return fmt.Errorf("unexpected orchestration VM replica ports: %v", m.Ports.VMBenchmarkSurface.Orchestration.OrderReplicas)
	}

	return nil
}

func withoutString(values []string, target string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			out = append(out, value)
		}
	}
	return out
}

func readRelativeFile(t *testing.T, relativePath string) string {
	t.Helper()
	data, err := os.ReadFile(relativePath)
	if err != nil {
		t.Fatalf("read %s: %v", relativePath, err)
	}
	return string(data)
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ", ")
}
