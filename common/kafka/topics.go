package kafka

const (
	DefaultOrderEventsTopic       = "order-events"
	DefaultPaymentEventsTopic     = "payment-events"
	DefaultInventoryEventsTopic   = "inventory-events"
	DefaultShippingEventsTopic    = "shipping-events"
	DefaultPaymentCommandsTopic   = "orchestration.payment.commands"
	DefaultInventoryCommandsTopic = "orchestration.inventory.commands"
	DefaultShippingCommandsTopic  = "orchestration.shipping.commands"
	DefaultPaymentRepliesTopic    = "orchestration.payment.replies"
	DefaultInventoryRepliesTopic  = "orchestration.inventory.replies"
	DefaultShippingRepliesTopic   = "orchestration.shipping.replies"
)

type Topics struct {
	OrderEvents       string
	PaymentEvents     string
	InventoryEvents   string
	ShippingEvents    string
	PaymentCommands   string
	InventoryCommands string
	ShippingCommands  string
	PaymentReplies    string
	InventoryReplies  string
	ShippingReplies   string
}

func DefaultTopics() Topics {
	return Topics{
		OrderEvents:       DefaultOrderEventsTopic,
		PaymentEvents:     DefaultPaymentEventsTopic,
		InventoryEvents:   DefaultInventoryEventsTopic,
		ShippingEvents:    DefaultShippingEventsTopic,
		PaymentCommands:   DefaultPaymentCommandsTopic,
		InventoryCommands: DefaultInventoryCommandsTopic,
		ShippingCommands:  DefaultShippingCommandsTopic,
		PaymentReplies:    DefaultPaymentRepliesTopic,
		InventoryReplies:  DefaultInventoryRepliesTopic,
		ShippingReplies:   DefaultShippingRepliesTopic,
	}
}

func NewTopics(overrides Topics) Topics {
	topics := DefaultTopics()
	if overrides.OrderEvents != "" {
		topics.OrderEvents = overrides.OrderEvents
	}
	if overrides.PaymentEvents != "" {
		topics.PaymentEvents = overrides.PaymentEvents
	}
	if overrides.InventoryEvents != "" {
		topics.InventoryEvents = overrides.InventoryEvents
	}
	if overrides.ShippingEvents != "" {
		topics.ShippingEvents = overrides.ShippingEvents
	}
	if overrides.PaymentCommands != "" {
		topics.PaymentCommands = overrides.PaymentCommands
	}
	if overrides.InventoryCommands != "" {
		topics.InventoryCommands = overrides.InventoryCommands
	}
	if overrides.ShippingCommands != "" {
		topics.ShippingCommands = overrides.ShippingCommands
	}
	if overrides.PaymentReplies != "" {
		topics.PaymentReplies = overrides.PaymentReplies
	}
	if overrides.InventoryReplies != "" {
		topics.InventoryReplies = overrides.InventoryReplies
	}
	if overrides.ShippingReplies != "" {
		topics.ShippingReplies = overrides.ShippingReplies
	}
	return topics
}
