package com.thesis.common.replies;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = PaymentCompletedReply.class, name = "PAYMENT_COMPLETED"),
    @JsonSubTypes.Type(value = PaymentFailedReply.class, name = "PAYMENT_FAILED"),
    @JsonSubTypes.Type(value = PaymentRefundedReply.class, name = "PAYMENT_REFUNDED"),
    @JsonSubTypes.Type(value = InventoryReservedReply.class, name = "INVENTORY_RESERVED"),
    @JsonSubTypes.Type(value = InventoryFailedReply.class, name = "INVENTORY_FAILED"),
    @JsonSubTypes.Type(value = InventoryReleasedReply.class, name = "INVENTORY_RELEASED"),
    @JsonSubTypes.Type(value = ShippingScheduledReply.class, name = "SHIPPING_SCHEDULED"),
    @JsonSubTypes.Type(value = ShippingFailedReply.class, name = "SHIPPING_FAILED"),
    @JsonSubTypes.Type(value = ShippingCancelledReply.class, name = "SHIPPING_CANCELLED")
})
public sealed interface SagaReply permits
    PaymentCompletedReply, PaymentFailedReply, PaymentRefundedReply,
    InventoryReservedReply, InventoryFailedReply, InventoryReleasedReply,
    ShippingScheduledReply, ShippingFailedReply, ShippingCancelledReply {

    String orderId();
}
