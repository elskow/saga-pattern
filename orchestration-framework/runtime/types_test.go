package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"saga-pattern/common/dto"
	"saga-pattern/common/testutil"
)

func TestDefinitionValidateRejectsDuplicateStepNames(t *testing.T) {
	def := validationDefinition(
		validationStepRef("Payment", "PAYMENT_PENDING"),
		validationStepRef("Payment", "INVENTORY_PENDING"),
	)

	err := def.Validate()
	if err == nil || !strings.Contains(err.Error(), `duplicate step name "Payment"`) {
		t.Fatalf("Validate() error = %v, want duplicate step name", err)
	}
}

func TestDefinitionValidateRejectsDuplicatePendingStates(t *testing.T) {
	def := validationDefinition(
		validationStepRef("Payment", "PENDING"),
		validationStepRef("Inventory", "PENDING"),
	)

	err := def.Validate()
	if err == nil || !strings.Contains(err.Error(), `duplicate pending state "PENDING"`) {
		t.Fatalf("Validate() error = %v, want duplicate pending state", err)
	}
}

func TestDefinitionValidateRejectsUnknownAdvanceTarget(t *testing.T) {
	payment := validationStepRef("Payment", "PAYMENT_PENDING")
	shipping := validationStepRef("Shipping", "SHIPPING_PENDING")
	def := validationDefinitionWithSteps(
		validationStepWithReplies(payment,
			On[testSagaData, struct{}]("payment.replies", "payment.completed", decodeEmptyReply).ThenAdvance("Missing", "MISSING_PENDING"),
			validationCompensationReply(payment.Name),
		),
		validationTerminalStep(shipping),
	)

	err := def.Validate()
	if err == nil || !strings.Contains(err.Error(), `advances to unknown step "Missing"`) {
		t.Fatalf("Validate() error = %v, want unknown step", err)
	}
}

func TestDefinitionValidateRejectsAdvanceStateMismatch(t *testing.T) {
	payment := validationStepRef("Payment", "PAYMENT_PENDING")
	inventory := validationStepRef("Inventory", "INVENTORY_PENDING")
	def := validationDefinitionWithSteps(
		validationStepWithReplies(payment,
			On[testSagaData, struct{}]("payment.replies", "payment.completed", decodeEmptyReply).ThenAdvance(inventory.Name, "WRONG_PENDING"),
			validationCompensationReply(payment.Name),
		),
		validationTerminalStep(inventory),
	)

	err := def.Validate()
	if err == nil || !strings.Contains(err.Error(), `advances to step "Inventory" with state "WRONG_PENDING"`) {
		t.Fatalf("Validate() error = %v, want mismatched next state", err)
	}
}

func TestDefinitionValidateRejectsDuplicateForwardReplyMatchers(t *testing.T) {
	payment := validationStepRef("Payment", "PAYMENT_PENDING")
	inventory := validationStepRef("Inventory", "INVENTORY_PENDING")
	def := validationDefinitionWithSteps(
		StepDefRef[testSagaData](payment).
			Forward(validationCommand("payment.commands", "process-payment")).
			Compensation(validationCommand("payment.commands", "refund-payment")).
			OnForward(
				On[testSagaData, struct{}]("payment.replies", "payment.completed", decodeEmptyReply).ThenAdvanceTo(inventory),
				On[testSagaData, struct{}]("payment.replies", "payment.completed", decodeEmptyReply).ThenAdvanceTo(inventory),
			).
			OnCompensate(validationCompensationReply(payment.Name)).
			Build(),
		validationTerminalStep(inventory),
	)

	err := def.Validate()
	if err == nil || !strings.Contains(err.Error(), `duplicate forward reply matcher`) {
		t.Fatalf("Validate() error = %v, want duplicate forward reply matcher", err)
	}
}

func TestDefinitionValidateRejectsDuplicateCompensationReplyMatchers(t *testing.T) {
	payment := validationStepRef("Payment", "PAYMENT_PENDING")
	inventory := validationStepRef("Inventory", "INVENTORY_PENDING")
	def := validationDefinitionWithSteps(
		StepDefRef[testSagaData](payment).
			Forward(validationCommand("payment.commands", "process-payment")).
			Compensation(validationCommand("payment.commands", "refund-payment")).
			OnForward(On[testSagaData, struct{}]("payment.replies", "payment.completed", decodeEmptyReply).ThenAdvanceTo(inventory)).
			OnCompensate(
				validationCompensationReply(payment.Name),
				validationCompensationReply(payment.Name),
			).
			Build(),
		validationTerminalStep(inventory),
	)

	err := def.Validate()
	if err == nil || !strings.Contains(err.Error(), `duplicate compensation reply matcher`) {
		t.Fatalf("Validate() error = %v, want duplicate compensation reply matcher", err)
	}
}

func TestDefinitionValidateAcceptsTypedAdvanceHelper(t *testing.T) {
	payment := validationStepRef("Payment", "PAYMENT_PENDING")
	inventory := validationStepRef("Inventory", "INVENTORY_PENDING")
	def := validationDefinition(payment, inventory)

	if err := def.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	db := testutil.OpenPostgres(t, testutil.DefaultOrderDatabaseURL, "framework_runtime_validation_test", testutil.Migration{Scope: "orchestration-framework", Dir: "orchestration-framework/db/migrations"})
	runtime, err := NewPostgres(def, db, PostgresDependencies{Publisher: newRecordingPublisher()})
	if err != nil {
		t.Fatalf("NewPostgres() error = %v", err)
	}
	if _, err := runtime.StartSaga(context.Background(), StartSagaInput[testSagaData]{Data: validTestSagaData()}); err != nil {
		t.Fatalf("StartSaga() error = %v", err)
	}
}

func validationDefinition(steps ...StepRef) Definition[testSagaData] {
	built := make([]Step[testSagaData], 0, len(steps))
	for idx, step := range steps {
		if idx == len(steps)-1 {
			built = append(built, validationTerminalStep(step))
			continue
		}
		built = append(built, validationStepWithReplies(step,
			On[testSagaData, struct{}](step.Name+".replies", step.Name+".completed", decodeEmptyReply).ThenAdvanceTo(steps[idx+1]),
			validationCompensationReply(step.Name),
		))
	}
	return validationDefinitionWithSteps(built...)
}

func validationDefinitionWithSteps(steps ...Step[testSagaData]) Definition[testSagaData] {
	return Definition[testSagaData]{
		SagaType:          "ValidationSaga",
		CompensatingState: "COMPENSATING",
		TerminalStates:    []string{"COMPLETED", "CANCELLED"},
		DataCodec:         JSONCodec[testSagaData]{},
		ValidateData:      func(data testSagaData) error { return data.Validate() },
		Steps:             steps,
	}
}

func validationStepWithReplies(ref StepRef, forward ReplyCase[testSagaData], compensation ReplyCase[testSagaData]) Step[testSagaData] {
	return StepDefRef[testSagaData](ref).
		Forward(validationCommand(ref.Name+".commands", "forward-"+strings.ToLower(ref.Name))).
		Compensation(validationCommand(ref.Name+".commands", "compensate-"+strings.ToLower(ref.Name))).
		OnForward(forward).
		OnCompensate(compensation).
		Build()
}

func validationTerminalStep(ref StepRef) Step[testSagaData] {
	return StepDefRef[testSagaData](ref).
		Forward(validationCommand(ref.Name+".commands", "forward-"+strings.ToLower(ref.Name))).
		Compensation(validationCommand(ref.Name+".commands", "compensate-"+strings.ToLower(ref.Name))).
		OnForward(On[testSagaData, struct{}](ref.Name+".replies", ref.Name+".completed", decodeEmptyReply).ThenComplete("COMPLETED")).
		OnCompensate(validationCompensationReply(ref.Name)).
		Build()
}

func validationCompensationReply(stepName string) ReplyCase[testSagaData] {
	return On[testSagaData, struct{}](stepName+".replies", stepName+".refunded", decodeEmptyReply).ThenCompensatedIf(func(struct{}) bool { return true })
}

func validationCommand(topic string, commandType string) CommandSpec[testSagaData] {
	return Command(topic, commandType, func(testSagaData) (any, error) {
		return map[string]string{"command": commandType}, nil
	})
}

func validationStepRef(name string, pendingState string) StepRef {
	return StepRef{Name: name, PendingState: pendingState}
}

func decodeEmptyReply([]byte) (struct{}, error) {
	return struct{}{}, nil
}

func validTestSagaData() testSagaData {
	return testSagaData{
		OrderID:         "ORDER-1",
		CustomerID:      "CUST-1",
		PaymentID:       "PAY-1",
		ReservationID:   "RES-1",
		ShippingID:      "SHIP-1",
		ShippingAddress: "Jl. Ketintang Wiyata, Surabaya 60231",
		TotalAmount:     json.Number("1599000"),
		Items: []dto.OrderItemRequest{{
			ProductID:   "PROD-1",
			ProductName: "Widget",
			Quantity:    1,
			Price:       json.Number("1599000"),
		}},
	}
}
