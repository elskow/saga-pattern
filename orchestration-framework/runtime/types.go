package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/common/commands"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-framework/internal/model"
	"saga-pattern/orchestration-framework/internal/store"
)

type Message struct {
	Topic       string
	Key         string
	MessageType string
	Payload     []byte
	SagaID      string
	Step        string
	Direction   string
}

type ReplyEnvelope struct {
	ReplyID    string
	SagaID     string
	Topic      string
	ReceivedAt time.Time
	Payload    []byte
}

type Publisher interface {
	Publish(context.Context, Message) error
}

type BootstrapDependencies struct {
	Publisher       Publisher
	MetricsRegistry *prometheus.Registry
	Clock           func() time.Time
	IDGenerator     func() string
	WorkerID        string
	Config          Config
}

type InMemoryDependencies = BootstrapDependencies
type PostgresDependencies = BootstrapDependencies

type Snapshot struct {
	SagaID             string
	SagaType           string
	State              string
	CurrentStep        string
	PendingDirection   string
	PendingCommandType string
	PendingReplyType   string
	LastError          string
	StartedAt          time.Time
	UpdatedAt          time.Time
	DeadlineAt         time.Time
	StepDeadlineAt     time.Time
	RetryCount         int
	MaxRetryCount      int
	OrderID            string
	CustomerID         string
	PaymentID          string
	ReservationID      string
	ShippingID         string
	CorrelationID      string
	ShippingAddress    string
	TotalAmount        json.Number
	Items              []dto.OrderItemRequest
}

type SagaData struct {
	OrderID         string
	CustomerID      string
	PaymentID       string
	ReservationID   string
	ShippingID      string
	CorrelationID   string
	ShippingAddress string
	TotalAmount     json.Number
	Items           []dto.OrderItemRequest
}

type StartSagaInput struct {
	SagaID string
	Data   SagaData
}

type Definition struct {
	SagaType          string
	InitialState      model.SagaState
	CompensatingState model.SagaState
	TerminalStates    []model.SagaState
	Steps             []Step
}

type Step struct {
	Name         string
	PendingState model.SagaState
	Forward      CommandSpec
	Compensation CommandSpec
	Success      Transition
	Failure      Transition
}

type CommandSpec struct {
	Topic        string
	CommandType  string
	ReplyType    string
	BuildPayload func(SagaData) (any, error)
}

type Transition struct {
	ReplyType string
	NextState model.SagaState
}

type Config struct {
	PendingCommandRetryDelay time.Duration
	SagaTimeout              time.Duration
	TimeoutCheckInterval     time.Duration
	OutboxPublishInterval    time.Duration
	OutboxRetryDelay         time.Duration
	OutboxMaxAttempts        int
	OutboxBatchSize          int
	CleanupInterval          time.Duration
	ProcessedReplyRetention  time.Duration
	OutboxRetention          time.Duration
	KafkaSendTimeout         time.Duration
	LeaseTTL                 time.Duration
	MaxStepRetries           int
	ReplyRetention           time.Duration
}

func DefaultConfig() Config {
	return Config{
		PendingCommandRetryDelay: 10 * time.Second,
		SagaTimeout:              30 * time.Second,
		TimeoutCheckInterval:     10 * time.Second,
		OutboxPublishInterval:    time.Second,
		OutboxRetryDelay:         30 * time.Second,
		OutboxMaxAttempts:        5,
		OutboxBatchSize:          100,
		CleanupInterval:          time.Hour,
		ProcessedReplyRetention:  24 * time.Hour,
		OutboxRetention:          24 * time.Hour,
		KafkaSendTimeout:         10 * time.Second,
		LeaseTTL:                 5 * time.Second,
		MaxStepRetries:           1,
		ReplyRetention:           24 * time.Hour,
	}
}

type Dependencies struct {
	Store           store.Store
	Publisher       Publisher
	MetricsRegistry *prometheus.Registry
	Clock           func() time.Time
	IDGenerator     func() string
	WorkerID        string
	Config          Config
}

func toPublicSnapshot(row model.SagaInstanceRow) Snapshot {
	return Snapshot{
		SagaID:             row.ID,
		SagaType:           row.SagaType,
		State:              string(row.State),
		CurrentStep:        row.CurrentStep,
		PendingDirection:   string(row.PendingDirection),
		PendingCommandType: row.PendingCommandType,
		PendingReplyType:   row.PendingReplyType,
		LastError:          row.LastError,
		StartedAt:          row.StartedAt,
		UpdatedAt:          row.UpdatedAt,
		DeadlineAt:         row.DeadlineAt,
		StepDeadlineAt:     row.StepDeadlineAt,
		RetryCount:         row.RetryCount,
		MaxRetryCount:      row.MaxRetryCount,
		OrderID:            row.Data.OrderID,
		CustomerID:         row.Data.CustomerID,
		PaymentID:          row.Data.PaymentID,
		ReservationID:      row.Data.ReservationID,
		ShippingID:         row.Data.ShippingID,
		CorrelationID:      row.Data.CorrelationID,
		ShippingAddress:    row.Data.ShippingAddress,
		TotalAmount:        row.Data.TotalAmount,
		Items:              append([]dto.OrderItemRequest(nil), row.Data.Items...),
	}
}

func OrderDefinition() Definition {
	topics := commonkafka.DefaultTopics()
	return Definition{
		SagaType:          "OrderSaga",
		InitialState:      model.SagaStateCreated,
		CompensatingState: model.SagaStateCompensating,
		TerminalStates:    []model.SagaState{model.SagaStateCompleted, model.SagaStateCancelled},
		Steps: []Step{
			{
				Name:         "Payment",
				PendingState: model.SagaStatePaymentPending,
				Forward: CommandSpec{Topic: topics.PaymentCommands, CommandType: commands.CommandProcessPayment, ReplyType: commonreplies.TypePaymentCompleted, BuildPayload: func(data SagaData) (any, error) {
					return commands.NewProcessPaymentCommand(data.PaymentID, data.OrderID, data.CustomerID, data.TotalAmount), nil
				}},
				Compensation: CommandSpec{Topic: topics.PaymentCommands, CommandType: commands.CommandRefundPayment, ReplyType: commonreplies.TypePaymentRefunded, BuildPayload: func(data SagaData) (any, error) {
					return commands.NewRefundPaymentCommand(data.PaymentID, data.OrderID), nil
				}},
				Success: Transition{ReplyType: commonreplies.TypePaymentCompleted, NextState: model.SagaStateInventoryPending},
				Failure: Transition{ReplyType: commonreplies.TypePaymentFailed, NextState: model.SagaStateCancelled},
			},
			{
				Name:         "Inventory",
				PendingState: model.SagaStateInventoryPending,
				Forward: CommandSpec{Topic: topics.InventoryCommands, CommandType: commands.CommandReserveInventory, ReplyType: commonreplies.TypeInventoryReserved, BuildPayload: func(data SagaData) (any, error) {
					return commands.NewReserveInventoryCommand(data.ReservationID, data.OrderID, data.Items), nil
				}},
				Compensation: CommandSpec{Topic: topics.InventoryCommands, CommandType: commands.CommandReleaseInventory, ReplyType: commonreplies.TypeInventoryReleased, BuildPayload: func(data SagaData) (any, error) {
					return commands.NewReleaseInventoryCommand(data.ReservationID, data.OrderID), nil
				}},
				Success: Transition{ReplyType: commonreplies.TypeInventoryReserved, NextState: model.SagaStateShippingPending},
				Failure: Transition{ReplyType: commonreplies.TypeInventoryFailed, NextState: model.SagaStateCompensating},
			},
			{
				Name:         "Shipping",
				PendingState: model.SagaStateShippingPending,
				Forward: CommandSpec{Topic: topics.ShippingCommands, CommandType: commands.CommandScheduleShipping, ReplyType: commonreplies.TypeShippingScheduled, BuildPayload: func(data SagaData) (any, error) {
					return commands.NewScheduleShippingCommand(data.ShippingID, data.OrderID, data.ShippingAddress), nil
				}},
				Compensation: CommandSpec{Topic: topics.ShippingCommands, CommandType: commands.CommandCancelShipping, ReplyType: commonreplies.TypeShippingCancelled, BuildPayload: func(data SagaData) (any, error) {
					return commands.NewCancelShippingCommand(data.ShippingID, data.OrderID), nil
				}},
				Success: Transition{ReplyType: commonreplies.TypeShippingScheduled, NextState: model.SagaStateCompleted},
				Failure: Transition{ReplyType: commonreplies.TypeShippingFailed, NextState: model.SagaStateCompensating},
			},
		},
	}
}

func (d Definition) Validate() error {
	if d.SagaType == "" {
		return fmt.Errorf("saga type is required")
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("at least one step is required")
	}
	return nil
}

func (d SagaData) Validate() error {
	if d.OrderID == "" {
		return fmt.Errorf("order id is required")
	}
	if d.CustomerID == "" {
		return fmt.Errorf("customer id is required")
	}
	if d.TotalAmount == "" {
		return fmt.Errorf("total amount is required")
	}
	if d.ShippingAddress == "" {
		return fmt.Errorf("shipping address is required")
	}
	if len(d.Items) == 0 {
		return fmt.Errorf("items are required")
	}
	for _, item := range d.Items {
		if err := item.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (d SagaData) withDefaults() SagaData {
	copyItems := append([]dto.OrderItemRequest(nil), d.Items...)
	if d.CorrelationID == "" {
		d.CorrelationID = commoncontext.ResolveCorrelationID("")
	}
	d.Items = copyItems
	return d
}

func toModelData(data SagaData) model.SagaData {
	return model.SagaData{
		OrderID:         data.OrderID,
		CustomerID:      data.CustomerID,
		PaymentID:       data.PaymentID,
		ReservationID:   data.ReservationID,
		ShippingID:      data.ShippingID,
		CorrelationID:   data.CorrelationID,
		ShippingAddress: data.ShippingAddress,
		TotalAmount:     data.TotalAmount,
		Items:           append([]dto.OrderItemRequest(nil), data.Items...),
	}
}

func fromModelData(data model.SagaData) SagaData {
	return SagaData{
		OrderID:         data.OrderID,
		CustomerID:      data.CustomerID,
		PaymentID:       data.PaymentID,
		ReservationID:   data.ReservationID,
		ShippingID:      data.ShippingID,
		CorrelationID:   data.CorrelationID,
		ShippingAddress: data.ShippingAddress,
		TotalAmount:     data.TotalAmount,
		Items:           append([]dto.OrderItemRequest(nil), data.Items...),
	}
}
