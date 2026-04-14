package validate

import (
	"encoding/json"
	"fmt"
	"strings"
)

func NonBlank(value string, field string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s cannot be blank", field)
	}
	return nil
}

func Expected(value string, expected string, field string) error {
	if value != expected {
		return fmt.Errorf("%s must be %q", field, expected)
	}
	return nil
}

func PositiveInt(value int, field string) error {
	if value <= 0 {
		return fmt.Errorf("%s must be positive", field)
	}
	return nil
}

func PositiveNumber(value json.Number, field string) error {
	if strings.TrimSpace(value.String()) == "" {
		return fmt.Errorf("%s cannot be blank", field)
	}

	parsed, err := value.Float64()
	if err != nil {
		return fmt.Errorf("%s must be numeric: %w", field, err)
	}
	if parsed <= 0 {
		return fmt.Errorf("%s must be positive", field)
	}
	return nil
}
