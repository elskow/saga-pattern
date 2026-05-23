package saga

import (
	"testing"

	commonkafka "saga-pattern/common/kafka"
)

func TestDefinitionValidates(t *testing.T) {
	if err := Definition(commonkafka.DefaultTopics()).Validate(); err != nil {
		t.Fatalf("Definition().Validate() error = %v", err)
	}
}
