package tracking

import "strings"

const DefaultPrefix = "TRK-"

func NumberForShippingID(shippingID string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(shippingID))
	trimmed = strings.ReplaceAll(trimmed, "-", "")
	if len(trimmed) > 10 {
		trimmed = trimmed[:10]
	}
	if trimmed == "" {
		trimmed = "SHIPMENT"
	}
	return DefaultPrefix + trimmed
}
