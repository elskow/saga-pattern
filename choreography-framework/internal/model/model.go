package model

import "time"

type OutboxStatus string

const (
	OutboxStatusPending OutboxStatus = "pending"
	OutboxStatusSending OutboxStatus = "sending"
	OutboxStatusSent    OutboxStatus = "sent"
	OutboxStatusFailed  OutboxStatus = "failed"
)

type OutboxRow struct {
	ID           string
	Topic        string
	Key          string
	EventType    string
	Payload      []byte
	Status       OutboxStatus
	AvailableAt  time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
	AttemptCount int
	MaxAttempts  int
	LastError    string
	SentAt       *time.Time
	ClaimedBy    string
	ClaimedUntil *time.Time
	TraceHeaders map[string]string
}

type ProcessedEventRow struct {
	EventKey    string
	ProcessedAt time.Time
}

type SchedulerLeaseRow struct {
	Name        string
	Owner       string
	LeasedUntil time.Time
	UpdatedAt   time.Time
}
