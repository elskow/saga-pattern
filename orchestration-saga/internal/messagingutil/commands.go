package messagingutil

import (
	"encoding/json"
	"fmt"

	"saga-pattern/common/validate"
)

type CommandEnvelope struct {
	Topic string
	Key   string
	Value []byte
}

type validatable interface {
	Validate() error
}

func EnsureTopic(actual string, expected string, label string) error {
	if actual != expected {
		return fmt.Errorf("unsupported %s topic %q", label, actual)
	}
	return nil
}

func DecodeCommandType(payload []byte, label string) (string, error) {
	var meta struct {
		CommandType string `json:"commandType"`
	}
	if err := json.Unmarshal(payload, &meta); err != nil {
		return "", fmt.Errorf("decode %s command: %w", label, err)
	}
	return meta.CommandType, nil
}

func DecodeValidatedCommand[T validatable](payload []byte, action string) (T, error) {
	var command T
	if err := validate.UnmarshalStrictJSON(payload, &command); err != nil {
		return command, fmt.Errorf("decode %s command: %w", action, err)
	}
	if err := command.Validate(); err != nil {
		return command, fmt.Errorf("validate %s command: %w", action, err)
	}
	return command, nil
}
