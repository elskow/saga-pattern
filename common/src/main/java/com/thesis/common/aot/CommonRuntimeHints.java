package com.thesis.common.aot;

import com.thesis.common.command.CancelShippingCommand;
import com.thesis.common.command.ProcessPaymentCommand;
import com.thesis.common.command.RefundPaymentCommand;
import com.thesis.common.command.ReleaseInventoryCommand;
import com.thesis.common.command.ReserveInventoryCommand;
import com.thesis.common.command.ScheduleShippingCommand;
import com.thesis.common.dto.CreateOrderRequest;
import com.thesis.common.dto.KafkaTopics;
import com.thesis.common.dto.OrderResponse;
import com.thesis.common.dto.OrderStatus;
import com.thesis.common.events.InventoryReleasedEvent;
import com.thesis.common.events.InventoryReservationFailedEvent;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.OrderCancelledEvent;
import com.thesis.common.events.OrderCompletedEvent;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import com.thesis.common.events.ShippingCancelledEvent;
import com.thesis.common.events.ShippingFailedEvent;
import com.thesis.common.events.ShippingScheduledEvent;
import com.thesis.common.replies.InventoryFailedReply;
import com.thesis.common.replies.InventoryReleasedReply;
import com.thesis.common.replies.InventoryReservedReply;
import com.thesis.common.replies.PaymentCompletedReply;
import com.thesis.common.replies.PaymentFailedReply;
import com.thesis.common.replies.PaymentRefundedReply;
import com.thesis.common.replies.SagaReply;
import com.thesis.common.replies.ShippingCancelledReply;
import com.thesis.common.replies.ShippingFailedReply;
import com.thesis.common.replies.ShippingScheduledReply;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * GraalVM Native Image runtime hints for common module classes.
 * Registers reflection hints for events, commands, replies, and DTOs
 * to enable JSON serialization/deserialization at runtime.
 */
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
        // Order events
        hints.reflection().registerType(OrderCreatedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderCreatedEvent.OrderItemEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderCompletedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderCancelledEvent.class, SERIALIZATION_CATEGORIES);

        // Payment events
        hints.reflection().registerType(PaymentCompletedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentFailedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentRefundedEvent.class, SERIALIZATION_CATEGORIES);

        // Inventory events
        hints.reflection().registerType(InventoryReservedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryReservedEvent.ReservedItem.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryReservationFailedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryReleasedEvent.class, SERIALIZATION_CATEGORIES);

        // Shipping events
        hints.reflection().registerType(ShippingScheduledEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingFailedEvent.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingCancelledEvent.class, SERIALIZATION_CATEGORIES);
    }

    private void registerCommandHints(RuntimeHints hints) {
        // Payment commands
        hints.reflection().registerType(ProcessPaymentCommand.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(RefundPaymentCommand.class, SERIALIZATION_CATEGORIES);

        // Inventory commands (uses OrderCreatedEvent.OrderItemEvent for items)
        hints.reflection().registerType(ReserveInventoryCommand.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ReleaseInventoryCommand.class, SERIALIZATION_CATEGORIES);

        // Shipping commands
        hints.reflection().registerType(ScheduleShippingCommand.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(CancelShippingCommand.class, SERIALIZATION_CATEGORIES);
    }

    private void registerReplyHints(RuntimeHints hints) {
        // Base reply interface for polymorphic deserialization
        hints.reflection().registerType(SagaReply.class, SERIALIZATION_CATEGORIES);

        // Payment replies
        hints.reflection().registerType(PaymentCompletedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentFailedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(PaymentRefundedReply.class, SERIALIZATION_CATEGORIES);

        // Inventory replies
        hints.reflection().registerType(InventoryReservedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryFailedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(InventoryReleasedReply.class, SERIALIZATION_CATEGORIES);

        // Shipping replies
        hints.reflection().registerType(ShippingScheduledReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingFailedReply.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(ShippingCancelledReply.class, SERIALIZATION_CATEGORIES);
    }

    private void registerDtoHints(RuntimeHints hints) {
        // Request/Response DTOs
        hints.reflection().registerType(CreateOrderRequest.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(CreateOrderRequest.OrderItemRequest.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderResponse.class, SERIALIZATION_CATEGORIES);
        hints.reflection().registerType(OrderResponse.OrderItemResponse.class, SERIALIZATION_CATEGORIES);

        // Kafka topics
        hints.reflection().registerType(KafkaTopics.class, SERIALIZATION_CATEGORIES);

        // Enum
        hints.reflection().registerType(OrderStatus.class, SERIALIZATION_CATEGORIES);
    }
}
