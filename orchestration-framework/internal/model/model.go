package model

import "time"

type SagaState string

const (
	SagaStateCompleted SagaState = "COMPLETED"
	SagaStateCancelled SagaState = "CANCELLED"
)

type StepDirection string

const (
	StepDirectionForward      StepDirection = "forward"
	StepDirectionCompensation StepDirection = "compensation"
)

type StepStatus string

const (
	StepStatusPending     StepStatus = "pending"
	StepStatusSucceeded   StepStatus = "succeeded"
	StepStatusFailed      StepStatus = "failed"
	StepStatusTimedOut    StepStatus = "timed_out"
	StepStatusCompensated StepStatus = "compensated"
	StepStatusIgnored     StepStatus = "ignored"
)

type OutboxStatus string

const (
	OutboxStatusPending OutboxStatus = "pending"
	OutboxStatusSending OutboxStatus = "sending"
	OutboxStatusSent    OutboxStatus = "sent"
	OutboxStatusFailed  OutboxStatus = "failed"
)

type SagaInstanceRow struct {
	ID                 string
	SagaType           string
	State              SagaState
	CurrentStep        string
	PendingDirection   StepDirection
	PendingCommandType string
	PendingReplyType   string
	StartedAt          time.Time
	UpdatedAt          time.Time
	DeadlineAt         time.Time
	StepDeadlineAt     time.Time
	RetryCount         int
	MaxRetryCount      int
	LastError          string
	Data               []byte
	RequestID          string
	CorrelationID      string
	BenchmarkRun       string
	BenchmarkScene     string
	BenchmarkPhase     string
}

type StepHistoryRow struct {
	ID          string
	SagaID      string
	Step        string
	Direction   StepDirection
	Status      StepStatus
	CommandType string
	ReplyType   string
	Attempt     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
	Error       string
}

type OutboxRow struct {
	ID             string
	SagaID         string
	SagaType       string
	Step           string
	Direction      StepDirection
	Topic          string
	Key            string
	MessageType    string
	Payload        []byte
	Status         OutboxStatus
	AvailableAt    time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	AttemptCount   int
	MaxAttempts    int
	LastError      string
	SentAt         *time.Time
	ClaimedBy      string
	ClaimedUntil   *time.Time
	RequestID      string
	CorrelationID  string
	BenchmarkRun   string
	BenchmarkScene string
	BenchmarkPhase string
	TraceHeaders   map[string]string
}

type ProcessedReplyRow struct {
	ReplyID    string
	SagaID     string
	ReplyType  string
	RecordedAt time.Time
	ExpiresAt  time.Time
}

type SchedulerLeaseRow struct {
	Name        string
	Owner       string
	LeasedUntil time.Time
	UpdatedAt   time.Time
}
