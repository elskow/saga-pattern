package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"

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

type PostgresDependencies = BootstrapDependencies

// AdvancedDependencies exposes the raw store-backed constructor path.
// Most application code should use NewPostgres instead.
type AdvancedDependencies = Dependencies

type Codec[D any] interface {
	Marshal(D) ([]byte, error)
	Unmarshal([]byte) (D, error)
}

type JSONCodec[D any] struct{}

func (JSONCodec[D]) Marshal(data D) ([]byte, error) {
	return json.Marshal(data)
}

func (JSONCodec[D]) Unmarshal(raw []byte) (D, error) {
	var data D
	if len(raw) == 0 {
		return data, nil
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return data, err
	}
	return data, nil
}

type Snapshot[D any] struct {
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
	Data               D
}

type View[D any] struct {
	SagaID      string
	SagaType    string
	State       string
	CurrentStep string
	LastError   string
	StartedAt   time.Time
	UpdatedAt   time.Time
	Data        D
	StepHistory []StepHistoryEntry
}

type StepHistoryEntry struct {
	Step      string
	Direction string
	Status    string
	Error     string
}

type StartSagaInput[D any] struct {
	SagaID string
	Data   D
}

type Definition[D any] struct {
	SagaType          string
	CompensatingState string
	TerminalStates    []string
	DataCodec         Codec[D]
	ValidateData      func(D) error
	Steps             []Step[D]
}

type DefinitionBuilder[D any] struct {
	definition Definition[D]
}

type Step[D any] struct {
	Name              string
	PendingState      string
	Forward           CommandSpec[D]
	Compensation      CommandSpec[D]
	ForwardReplies    []ReplyCase[D]
	CompensateReplies []ReplyCase[D]
}

type StepRef struct {
	Name         string
	PendingState string
}

type StepBuilder[D any] struct {
	step Step[D]
}

type CommandSpec[D any] struct {
	Topic        string
	CommandType  string
	BuildPayload func(D) (any, error)
}

type ReplyCase[D any] struct {
	Topic     string
	ReplyType string
	Decide    func(D, []byte) (Decision, error)
	declared  *Decision
}

type ReplyCaseBuilder[D any, R any] struct {
	topic     string
	replyType string
	decode    func([]byte) (R, error)
}

type DecisionKind string

const (
	DecisionIgnore            DecisionKind = "ignore"
	DecisionAdvance           DecisionKind = "advance"
	DecisionComplete          DecisionKind = "complete"
	DecisionBeginCompensation DecisionKind = "begin_compensation"
	DecisionCancel            DecisionKind = "cancel"
	DecisionCompensated       DecisionKind = "compensated"
)

type Decision struct {
	Kind      DecisionKind
	NextStep  string
	NextState string
	Reason    string
}

func Ignore() Decision {
	return Decision{Kind: DecisionIgnore}
}

func Advance(nextStep string, nextState string) Decision {
	return Decision{Kind: DecisionAdvance, NextStep: nextStep, NextState: nextState}
}

func Complete(state string) Decision {
	return Decision{Kind: DecisionComplete, NextState: state}
}

func BeginCompensation(reason string) Decision {
	return Decision{Kind: DecisionBeginCompensation, Reason: reason}
}

func Cancel(state string, reason string) Decision {
	return Decision{Kind: DecisionCancel, NextState: state, Reason: reason}
}

func Compensated() Decision {
	return Decision{Kind: DecisionCompensated}
}

func Define[D any](sagaType string) *DefinitionBuilder[D] {
	return &DefinitionBuilder[D]{definition: Definition[D]{SagaType: sagaType}}
}

func (b *DefinitionBuilder[D]) Codec(codec Codec[D]) *DefinitionBuilder[D] {
	b.definition.DataCodec = codec
	return b
}

func (b *DefinitionBuilder[D]) Validate(validate func(D) error) *DefinitionBuilder[D] {
	b.definition.ValidateData = validate
	return b
}

func (b *DefinitionBuilder[D]) CompensatingState(state string) *DefinitionBuilder[D] {
	b.definition.CompensatingState = state
	return b
}

func (b *DefinitionBuilder[D]) TerminalStates(states ...string) *DefinitionBuilder[D] {
	b.definition.TerminalStates = append([]string(nil), states...)
	return b
}

func (b *DefinitionBuilder[D]) Step(step Step[D]) *DefinitionBuilder[D] {
	b.definition.Steps = append(b.definition.Steps, step)
	return b
}

func (b *DefinitionBuilder[D]) Build() Definition[D] {
	return b.definition
}

func StepDef[D any](name string, pendingState string) *StepBuilder[D] {
	return &StepBuilder[D]{step: Step[D]{Name: name, PendingState: pendingState}}
}

func StepDefRef[D any](ref StepRef) *StepBuilder[D] {
	return StepDef[D](ref.Name, ref.PendingState)
}

func (b *StepBuilder[D]) Forward(command CommandSpec[D]) *StepBuilder[D] {
	b.step.Forward = command
	return b
}

func (b *StepBuilder[D]) Compensation(command CommandSpec[D]) *StepBuilder[D] {
	b.step.Compensation = command
	return b
}

func (b *StepBuilder[D]) OnForward(cases ...ReplyCase[D]) *StepBuilder[D] {
	b.step.ForwardReplies = append(b.step.ForwardReplies, cases...)
	return b
}

func (b *StepBuilder[D]) OnCompensate(cases ...ReplyCase[D]) *StepBuilder[D] {
	b.step.CompensateReplies = append(b.step.CompensateReplies, cases...)
	return b
}

func (b *StepBuilder[D]) Build() Step[D] {
	return b.step
}

func Command[D any](topic string, commandType string, build func(D) (any, error)) CommandSpec[D] {
	return CommandSpec[D]{Topic: topic, CommandType: commandType, BuildPayload: build}
}

func On[D any, R any](topic string, replyType string, decode func([]byte) (R, error)) ReplyCaseBuilder[D, R] {
	return ReplyCaseBuilder[D, R]{topic: topic, replyType: replyType, decode: decode}
}

func (b ReplyCaseBuilder[D, R]) Then(decide func(D, R) (Decision, error)) ReplyCase[D] {
	return TypedReplyCase(b.topic, b.replyType, b.decode, decide)
}

func (b ReplyCaseBuilder[D, R]) ThenAdvance(nextStep string, nextState string) ReplyCase[D] {
	decision := Advance(nextStep, nextState)
	return ReplyCase[D]{
		Topic:     b.topic,
		ReplyType: b.replyType,
		Decide: func(_ D, payload []byte) (Decision, error) {
			if _, err := b.decode(payload); err != nil {
				return Decision{}, err
			}
			return decision, nil
		},
		declared: &decision,
	}
}

func (b ReplyCaseBuilder[D, R]) ThenAdvanceTo(ref StepRef) ReplyCase[D] {
	return b.ThenAdvance(ref.Name, ref.PendingState)
}

func (b ReplyCaseBuilder[D, R]) ThenComplete(state string) ReplyCase[D] {
	decision := Complete(state)
	return ReplyCase[D]{
		Topic:     b.topic,
		ReplyType: b.replyType,
		Decide: func(_ D, payload []byte) (Decision, error) {
			if _, err := b.decode(payload); err != nil {
				return Decision{}, err
			}
			return decision, nil
		},
		declared: &decision,
	}
}

func (b ReplyCaseBuilder[D, R]) ThenBeginCompensation(reason func(R) string) ReplyCase[D] {
	return ReplyCase[D]{
		Topic:     b.topic,
		ReplyType: b.replyType,
		Decide: func(_ D, payload []byte) (Decision, error) {
			reply, err := b.decode(payload)
			if err != nil {
				return Decision{}, err
			}
			decision := BeginCompensation(reason(reply))
			return decision, nil
		},
		declared: &Decision{Kind: DecisionBeginCompensation},
	}
}

func (b ReplyCaseBuilder[D, R]) ThenCancel(state string, reason func(R) string) ReplyCase[D] {
	return ReplyCase[D]{
		Topic:     b.topic,
		ReplyType: b.replyType,
		Decide: func(_ D, payload []byte) (Decision, error) {
			reply, err := b.decode(payload)
			if err != nil {
				return Decision{}, err
			}
			decision := Cancel(state, reason(reply))
			return decision, nil
		},
		declared: &Decision{Kind: DecisionCancel, NextState: state},
	}
}

func (b ReplyCaseBuilder[D, R]) ThenCompensatedIf(success func(R) bool) ReplyCase[D] {
	return ReplyCase[D]{
		Topic:     b.topic,
		ReplyType: b.replyType,
		Decide: func(_ D, payload []byte) (Decision, error) {
			reply, err := b.decode(payload)
			if err != nil {
				return Decision{}, err
			}
			if !success(reply) {
				return Ignore(), nil
			}
			return Compensated(), nil
		},
		declared: &Decision{Kind: DecisionCompensated},
	}
}

func TypedReplyCase[D any, R any](topic string, replyType string, decode func([]byte) (R, error), decide func(D, R) (Decision, error)) ReplyCase[D] {
	return ReplyCase[D]{
		Topic:     topic,
		ReplyType: replyType,
		Decide: func(data D, payload []byte) (Decision, error) {
			reply, err := decode(payload)
			if err != nil {
				return Decision{}, err
			}
			return decide(data, reply)
		},
	}
}

type Config struct {
	PendingCommandRetryDelay time.Duration
	SagaTimeout              time.Duration
	TimeoutCheckInterval     time.Duration
	OutboxPublishInterval    time.Duration
	ImmediateOutboxPublish   bool
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
		ImmediateOutboxPublish:   true,
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

func (d Definition[D]) Validate() error {
	if d.SagaType == "" {
		return fmt.Errorf("saga type is required")
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("at least one step is required")
	}
	stepsByName := make(map[string]Step[D], len(d.Steps))
	stepNameSeen := make(map[string]struct{}, len(d.Steps))
	pendingStateSeen := make(map[string]string, len(d.Steps))
	for _, step := range d.Steps {
		if step.Name == "" {
			return fmt.Errorf("step name is required")
		}
		if _, exists := stepNameSeen[step.Name]; exists {
			return fmt.Errorf("duplicate step name %q", step.Name)
		}
		stepNameSeen[step.Name] = struct{}{}
		if step.PendingState == "" {
			return fmt.Errorf("pending state is required for step %s", step.Name)
		}
		if existingStep, exists := pendingStateSeen[step.PendingState]; exists {
			return fmt.Errorf("duplicate pending state %q for steps %s and %s", step.PendingState, existingStep, step.Name)
		}
		pendingStateSeen[step.PendingState] = step.Name
		stepsByName[step.Name] = step
		if step.Forward.Topic == "" || step.Forward.CommandType == "" || step.Forward.BuildPayload == nil {
			return fmt.Errorf("forward command is incomplete for step %s", step.Name)
		}
		if len(step.ForwardReplies) == 0 {
			return fmt.Errorf("forward reply cases are required for step %s", step.Name)
		}
		forwardMatchers := make(map[string]struct{}, len(step.ForwardReplies))
		for _, reply := range step.ForwardReplies {
			if err := validateReplyCase(step.Name, "forward", reply); err != nil {
				return err
			}
			matcherKey := reply.Topic + "\x00" + reply.ReplyType
			if _, exists := forwardMatchers[matcherKey]; exists {
				return fmt.Errorf("duplicate forward reply matcher for step %s: topic=%q replyType=%q", step.Name, reply.Topic, reply.ReplyType)
			}
			forwardMatchers[matcherKey] = struct{}{}
		}
		if step.Compensation.Topic == "" || step.Compensation.CommandType == "" || step.Compensation.BuildPayload == nil {
			return fmt.Errorf("compensation command is incomplete for step %s", step.Name)
		}
		if len(step.CompensateReplies) == 0 {
			return fmt.Errorf("compensation reply cases are required for step %s", step.Name)
		}
		compensationMatchers := make(map[string]struct{}, len(step.CompensateReplies))
		for _, reply := range step.CompensateReplies {
			if err := validateReplyCase(step.Name, "compensation", reply); err != nil {
				return err
			}
			matcherKey := reply.Topic + "\x00" + reply.ReplyType
			if _, exists := compensationMatchers[matcherKey]; exists {
				return fmt.Errorf("duplicate compensation reply matcher for step %s: topic=%q replyType=%q", step.Name, reply.Topic, reply.ReplyType)
			}
			compensationMatchers[matcherKey] = struct{}{}
		}
	}
	for _, step := range d.Steps {
		for _, reply := range step.ForwardReplies {
			if err := validateDeclaredDecision(step.Name, stepsByName, reply); err != nil {
				return err
			}
		}
		for _, reply := range step.CompensateReplies {
			if err := validateDeclaredDecision(step.Name, stepsByName, reply); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d Definition[D]) codec() Codec[D] {
	if d.DataCodec != nil {
		return d.DataCodec
	}
	return JSONCodec[D]{}
}

func (d Definition[D]) ReplyTopics() []string {
	seen := make(map[string]struct{})
	for _, step := range d.Steps {
		for _, reply := range step.ForwardReplies {
			seen[reply.Topic] = struct{}{}
		}
		for _, reply := range step.CompensateReplies {
			seen[reply.Topic] = struct{}{}
		}
	}
	topics := make([]string, 0, len(seen))
	for topic := range seen {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

func ReplyType(payload []byte) (string, error) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return "", err
	}
	if envelope.Type == "" {
		return "", fmt.Errorf("reply type is required")
	}
	return envelope.Type, nil
}

func validateReplyCase[D any](step string, phase string, reply ReplyCase[D]) error {
	if reply.Topic == "" {
		return fmt.Errorf("%s reply topic is required for step %s", phase, step)
	}
	if reply.ReplyType == "" {
		return fmt.Errorf("%s reply type is required for step %s", phase, step)
	}
	if reply.Decide == nil {
		return fmt.Errorf("%s reply decision is required for step %s", phase, step)
	}
	return nil
}

func validateDeclaredDecision[D any](step string, stepsByName map[string]Step[D], reply ReplyCase[D]) error {
	if reply.declared == nil {
		return nil
	}
	if reply.declared.Kind != DecisionAdvance {
		return nil
	}
	targetStep, ok := stepsByName[reply.declared.NextStep]
	if !ok {
		return fmt.Errorf("step %s advances to unknown step %q", step, reply.declared.NextStep)
	}
	if reply.declared.NextState != targetStep.PendingState {
		return fmt.Errorf("step %s advances to step %q with state %q, but pending state is %q", step, targetStep.Name, reply.declared.NextState, targetStep.PendingState)
	}
	return nil
}
