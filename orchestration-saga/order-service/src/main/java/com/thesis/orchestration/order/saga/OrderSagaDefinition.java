package com.thesis.orchestration.order.saga;

import com.thesis.common.command.*;
import com.thesis.common.replies.*;
import com.thesis.orchestration.order.statemachine.OrderEvents;
import com.thesis.orchestration.order.statemachine.OrderStates;
import com.thesis.saga.core.SagaDefinition;
import com.thesis.saga.dsl.SagaBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrderSagaDefinition {

    public static final String PAYMENT_COMMAND_TOPIC = "orchestration.payment.commands";
    public static final String INVENTORY_COMMAND_TOPIC = "orchestration.inventory.commands";
    public static final String SHIPPING_COMMAND_TOPIC = "orchestration.shipping.commands";

    public static final String COMMAND_PROCESS_PAYMENT = "PROCESS_PAYMENT";
    public static final String COMMAND_REFUND_PAYMENT = "REFUND_PAYMENT";
    public static final String COMMAND_RESERVE_INVENTORY = "RESERVE_INVENTORY";
    public static final String COMMAND_RELEASE_INVENTORY = "RELEASE_INVENTORY";
    public static final String COMMAND_SCHEDULE_SHIPPING = "SCHEDULE_SHIPPING";
    public static final String COMMAND_CANCEL_SHIPPING = "CANCEL_SHIPPING";

    @Bean
    public SagaDefinition<OrderStates, OrderEvents, OrderSagaContext> orderSaga() {
        return SagaBuilder.<OrderStates, OrderEvents, OrderSagaContext>create("OrderSaga")
                .withStateClass(OrderStates.class)
                .withEventClass(OrderEvents.class)
                .withContextClass(OrderSagaContext.class)
                .initialState(OrderStates.CREATED)
                .terminalStates(OrderStates.COMPLETED, OrderStates.CANCELLED)
                .compensatingState(OrderStates.COMPENSATING)
                .startEvent(OrderEvents.START_SAGA)
                .compensationCompleteEvent(OrderEvents.COMPENSATION_COMPLETE)

                .step("Payment")
                    .onState(OrderStates.PAYMENT_PENDING)
                    .sendCommand(COMMAND_PROCESS_PAYMENT, PAYMENT_COMMAND_TOPIC)
                    .withPayload(ctx -> new ProcessPaymentCommand(
                            COMMAND_PROCESS_PAYMENT,
                            ctx.getPaymentId(),
                            ctx.getOrderId(),
                            ctx.getCustomerId(),
                            ctx.getTotalAmount()))
                    .onSuccess(OrderEvents.PAYMENT_SUCCESS, OrderStates.INVENTORY_PENDING)
                    .onFailure(OrderEvents.PAYMENT_FAILED, OrderStates.CANCELLED)
                    .withCompensation(COMMAND_REFUND_PAYMENT, PAYMENT_COMMAND_TOPIC)
                    .compensationPayload(ctx -> new RefundPaymentCommand(
                            COMMAND_REFUND_PAYMENT,
                            ctx.getPaymentId(),
                            ctx.getOrderId()))
                    .successReplyType(PaymentCompletedReply.class)
                    .failureReplyType(PaymentFailedReply.class)
                    .compensationReplyType(PaymentRefundedReply.class)
                    .isCompleted(OrderSagaContext::isPaymentCompleted)
                    .endStep()

                .step("Inventory")
                    .onState(OrderStates.INVENTORY_PENDING)
                    .sendCommand(COMMAND_RESERVE_INVENTORY, INVENTORY_COMMAND_TOPIC)
                    .withPayload(ctx -> new ReserveInventoryCommand(
                            COMMAND_RESERVE_INVENTORY,
                            ctx.getReservationId(),
                            ctx.getOrderId(),
                            ctx.getItems()))
                    .onSuccess(OrderEvents.INVENTORY_RESERVED, OrderStates.SHIPPING_PENDING)
                    .onFailure(OrderEvents.INVENTORY_FAILED, OrderStates.COMPENSATING)
                    .withCompensation(COMMAND_RELEASE_INVENTORY, INVENTORY_COMMAND_TOPIC)
                    .compensationPayload(ctx -> new ReleaseInventoryCommand(
                            COMMAND_RELEASE_INVENTORY,
                            ctx.getReservationId(),
                            ctx.getOrderId()))
                    .successReplyType(InventoryReservedReply.class)
                    .failureReplyType(InventoryFailedReply.class)
                    .compensationReplyType(InventoryReleasedReply.class)
                    .isCompleted(OrderSagaContext::isInventoryReserved)
                    .endStep()

                .step("Shipping")
                    .onState(OrderStates.SHIPPING_PENDING)
                    .sendCommand(COMMAND_SCHEDULE_SHIPPING, SHIPPING_COMMAND_TOPIC)
                    .withPayload(ctx -> new ScheduleShippingCommand(
                            COMMAND_SCHEDULE_SHIPPING,
                            ctx.getShipmentId(),
                            ctx.getOrderId(),
                            ctx.getShippingAddress()))
                    .onSuccess(OrderEvents.SHIPPING_SCHEDULED, OrderStates.COMPLETED)
                    .onFailure(OrderEvents.SHIPPING_FAILED, OrderStates.COMPENSATING)
                    .withCompensation(COMMAND_CANCEL_SHIPPING, SHIPPING_COMMAND_TOPIC)
                    .compensationPayload(ctx -> new CancelShippingCommand(
                            COMMAND_CANCEL_SHIPPING,
                            ctx.getShipmentId(),
                            ctx.getOrderId()))
                    .successReplyType(ShippingScheduledReply.class)
                    .failureReplyType(ShippingFailedReply.class)
                    .compensationReplyType(ShippingCancelledReply.class)
                    .isCompleted(OrderSagaContext::isShippingScheduled)
                    .endStep()

                .build();
    }
}
