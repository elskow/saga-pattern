package com.thesis.orchestration.order.saga;

import com.thesis.orchestration.order.dto.CreateOrderRequest;
import com.thesis.orchestration.order.service.OrderService;
import io.eventuate.tram.sagas.orchestration.SagaInstanceFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderSagaManager {

    private final SagaInstanceFactory sagaInstanceFactory;
    private final OrderSaga orderSaga;
    private final OrderService orderService;

    public String createOrder(CreateOrderRequest request) {
        // Generate IDs
        String orderId = UUID.randomUUID().toString();
        String paymentId = UUID.randomUUID().toString();
        String reservationId = UUID.randomUUID().toString();
        String shipmentId = UUID.randomUUID().toString();

        // Create and persist order entity FIRST
        orderService.createOrder(
                orderId,
                request.getCustomerId(),
                request.getTotalAmount(),
                request.getShippingAddress(),
                request.getItems(),
                paymentId,
                reservationId,
                shipmentId
        );

        // Build saga data
        OrderSagaData sagaData = OrderSagaData.builder()
                .orderId(orderId)
                .customerId(request.getCustomerId())
                .paymentId(paymentId)
                .reservationId(reservationId)
                .shipmentId(shipmentId)
                .totalAmount(request.getTotalAmount())
                .shippingAddress(request.getShippingAddress())
                .items(request.getItems())
                .paymentCompleted(false)
                .inventoryReserved(false)
                .shippingScheduled(false)
                .build();

        log.info("Creating saga for order: {}", orderId);

        // Create and start saga with error handling
        try {
            sagaInstanceFactory.create(orderSaga, sagaData);
        } catch (Exception e) {
            log.error("Failed to create saga for order {}: {}", orderId, e.getMessage());
            orderService.failOrder(orderId, "Saga creation failed: " + e.getMessage());
            throw new RuntimeException("Failed to create order saga", e);
        }

        return orderId;
    }
}
