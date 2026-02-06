package com.thesis.common.aot;

import com.thesis.common.command.*;
import com.thesis.common.config.KafkaErrorHandlingProperties;
import com.thesis.common.config.KafkaTopicsProperties;
import com.thesis.common.config.PaymentProperties;
import com.thesis.common.config.ShippingProperties;
import com.thesis.common.dto.CreateOrderRequest;
import com.thesis.common.dto.OrderResponse;
import com.thesis.common.dto.OrderStatus;
import com.thesis.common.events.*;
import com.thesis.common.replies.*;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

public class CommonRuntimeHints implements RuntimeHintsRegistrar {

    private static final MemberCategory[] SERIALIZATION_CATEGORIES = {
        MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
        MemberCategory.INVOKE_DECLARED_METHODS,
        MemberCategory.DECLARED_FIELDS
    };

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        registerEventHints(hints);
        registerCommandHints(hints);
        registerReplyHints(hints);
        registerDtoHints(hints);
    }

    private void registerEventHints(RuntimeHints hints) {
        hints.reflection().registerType(OrderCreatedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderCreatedEvent.OrderItemEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderCompletedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderCancelledEvent.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(PaymentCompletedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentFailedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentRefundedEvent.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(InventoryReservedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryReservedEvent.ReservedItem.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryReservationFailedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryReleasedEvent.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(ShippingScheduledEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingFailedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingCancelledEvent.class, SERIALIZATION_CATEGORIES);
    }

    private void registerCommandHints(RuntimeHints hints) {
        hints.reflection().registerType(ProcessPaymentCommand.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(RefundPaymentCommand.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(ReserveInventoryCommand.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ReleaseInventoryCommand.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(ScheduleShippingCommand.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(CancelShippingCommand.class, SERIALIZATION_CATEGORIES);
    }

    private void registerReplyHints(RuntimeHints hints) {
        hints.reflection().registerType(SagaReply.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(PaymentCompletedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentFailedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentRefundedReply.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(InventoryReservedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryFailedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryReleasedReply.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(ShippingScheduledReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingFailedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingCancelledReply.class, SERIALIZATION_CATEGORIES);
    }

    private void registerDtoHints(RuntimeHints hints) {
        hints.reflection().registerType(CreateOrderRequest.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(CreateOrderRequest.OrderItemRequest.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderResponse.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderResponse.OrderItemResponse.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(KafkaTopicsProperties.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(KafkaErrorHandlingProperties.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentProperties.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingProperties.class, SERIALIZATION_CATEGORIES);

        hints.reflection().registerType(OrderStatus.class, SERIALIZATION_CATEGORIES);
    }
}
