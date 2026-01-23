package com.thesis.common.replies;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base interface for all saga reply messages.
 * Uses Jackson polymorphic type handling for clean deserialization.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = PaymentCompletedReply.class, name = "PAYMENT_COMPLETED"),
    @JsonSubTypes.Type(value = PaymentFailedReply.class, name = "PAYMENT_FAILED"),
    @JsonSubTypes.Type(value = InventoryReservedReply.class, name = "INVENTORY_RESERVED"),
    @JsonSubTypes.Type(value = InventoryFailedReply.class, name = "INVENTORY_FAILED"),
    @JsonSubTypes.Type(value = ShippingScheduledReply.class, name = "SHIPPING_SCHEDULED"),
    @JsonSubTypes.Type(value = ShippingFailedReply.class, name = "SHIPPING_FAILED")
})
public interface SagaReply {
    String getOrderId();
}
