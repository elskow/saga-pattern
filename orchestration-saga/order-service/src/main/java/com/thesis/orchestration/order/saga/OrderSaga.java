package com.thesis.orchestration.order.saga;

import com.thesis.common.command.*;
import com.thesis.common.replies.*;
import com.thesis.orchestration.order.service.OrderService;
import io.eventuate.tram.commands.consumer.CommandWithDestination;
import io.eventuate.tram.sagas.orchestration.SagaDefinition;
import io.eventuate.tram.sagas.simpledsl.SimpleSaga;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

import static io.eventuate.tram.commands.consumer.CommandWithDestinationBuilder.send;


@Component
@Slf4j
@RequiredArgsConstructor
public class OrderSaga implements SimpleSaga<OrderSagaData> {

    private final OrderService orderService;
    private SagaDefinition<OrderSagaData> sagaDefinition;

    @PostConstruct
    public void initializeSagaDefinition() {
        this.sagaDefinition = step()
            .invokeParticipant(this::processPayment)
            .onReply(PaymentCompletedReply.class, this::handlePaymentCompleted)
            .onReply(PaymentFailedReply.class, this::handlePaymentFailed)
            .withCompensation(this::refundPayment)
        .step()
            .invokeParticipant(this::reserveInventory)
            .onReply(InventoryReservedReply.class, this::handleInventoryReserved)
            .onReply(InventoryFailedReply.class, this::handleInventoryFailed)
            .withCompensation(this::releaseInventory)
        .step()
            .invokeParticipant(this::scheduleShipping)
            .onReply(ShippingScheduledReply.class, this::handleShippingScheduled)
            .onReply(ShippingFailedReply.class, this::handleShippingFailed)
            .withCompensation(this::cancelShipping)
        .build();
    }

    @Override
    public SagaDefinition<OrderSagaData> getSagaDefinition() {
        return sagaDefinition;
    }

    // ========== STEP 1: Process Payment ==========

    private CommandWithDestination processPayment(OrderSagaData data) {
        log.info("Saga started for order: {}. Processing payment...", data.getOrderId());

        return send(ProcessPaymentCommand.builder()
                .paymentId(data.getPaymentId())
                .orderId(data.getOrderId())
                .customerId(data.getCustomerId())
                .amount(data.getTotalAmount())
                .build())
            .to("payment-service")
            .build();
    }

    private void handlePaymentCompleted(OrderSagaData data, PaymentCompletedReply reply) {
        log.info("Payment {} completed for order: {}", reply.getPaymentId(), data.getOrderId());
        data.setPaymentCompleted(true);
    }

    private void handlePaymentFailed(OrderSagaData data, PaymentFailedReply reply) {
        log.error("Payment {} failed for order: {}. Reason: {}",
            reply.getPaymentId(), data.getOrderId(), reply.getReason());
        // Update order status to fail
        orderService.failOrder(data.getOrderId(), "Payment failed: " + reply.getReason());
    }

    private CommandWithDestination refundPayment(OrderSagaData data) {
        log.info("Refunding payment {} for order: {}", data.getPaymentId(), data.getOrderId());

        return send(RefundPaymentCommand.builder()
                .paymentId(data.getPaymentId())
                .orderId(data.getOrderId())
                .build())
            .to("payment-service")
            .build();
    }

    // ========== STEP 2: Reserve Inventory ==========

    private CommandWithDestination reserveInventory(OrderSagaData data) {
        log.info("Reserving inventory for order: {}", data.getOrderId());

        return send(ReserveInventoryCommand.builder()
                .reservationId(data.getReservationId())
                .orderId(data.getOrderId())
                .items(data.getItems())
                .build())
            .to("inventory-service")
            .build();
    }

    private void handleInventoryReserved(OrderSagaData data, InventoryReservedReply reply) {
        log.info("Inventory {} reserved for order: {}", reply.getReservationId(), data.getOrderId());
        data.setInventoryReserved(true);
    }

    private void handleInventoryFailed(OrderSagaData data, InventoryFailedReply reply) {
        log.error("Inventory reservation {} failed for order: {}. Reason: {}",
            reply.getReservationId(), data.getOrderId(), reply.getReason());
        // Update order status to fail (saga will trigger compensations)
        orderService.failOrder(data.getOrderId(), "Inventory reservation failed: " + reply.getReason());
    }

    private CommandWithDestination releaseInventory(OrderSagaData data) {
        log.info("Releasing inventory {} for order: {}", data.getReservationId(), data.getOrderId());

        return send(ReleaseInventoryCommand.builder()
                .reservationId(data.getReservationId())
                .orderId(data.getOrderId())
                .build())
            .to("inventory-service")
            .build();
    }

    // ========== STEP 3: Schedule Shipping ==========

    private CommandWithDestination scheduleShipping(OrderSagaData data) {
        log.info("Scheduling shipping for order: {}", data.getOrderId());

        return send(ScheduleShippingCommand.builder()
                .shipmentId(data.getShipmentId())
                .orderId(data.getOrderId())
                .shippingAddress(data.getShippingAddress())
                .build())
            .to("shipping-service")
            .build();
    }

    private void handleShippingScheduled(OrderSagaData data, ShippingScheduledReply reply) {
        log.info("Shipping {} scheduled for order: {}. Order completed successfully!",
            reply.getShipmentId(), data.getOrderId());
        data.setShippingScheduled(true);
        // Mark order as completed
        orderService.completeOrder(data.getOrderId());
    }

    private void handleShippingFailed(OrderSagaData data, ShippingFailedReply reply) {
        log.error("Shipping {} failed for order: {}. Reason: {}",
            reply.getShipmentId(), data.getOrderId(), reply.getReason());
        // Update order status to fail (saga will trigger compensations)
        orderService.failOrder(data.getOrderId(), "Shipping failed: " + reply.getReason());
    }

    private CommandWithDestination cancelShipping(OrderSagaData data) {
        log.info("Cancelling shipping {} for order: {}", data.getShipmentId(), data.getOrderId());

        return send(CancelShippingCommand.builder()
                .shipmentId(data.getShipmentId())
                .orderId(data.getOrderId())
                .build())
            .to("shipping-service")
            .build();
    }
}
