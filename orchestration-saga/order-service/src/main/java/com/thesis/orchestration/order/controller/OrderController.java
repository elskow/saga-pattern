package com.thesis.orchestration.order.controller;

import com.thesis.orchestration.order.dto.CreateOrderRequest;
import com.thesis.orchestration.order.model.OrderEntity;
import com.thesis.orchestration.order.statemachine.OrderSagaOrchestrator;
import com.thesis.orchestration.order.service.OrderService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderSagaOrchestrator orderSagaOrchestrator;
    private final OrderService orderService;
    private final MeterRegistry meterRegistry;

    @PostMapping
    public ResponseEntity<Map<String, Object>> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        log.info("Received create order request for customer: {}", request.getCustomerId());

        Counter.builder("orchestration.orders.created")
                .tag("service", "order-service")
                .register(meterRegistry)
                .increment();

        String orderId = orderSagaOrchestrator.createOrder(request);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", orderId,
                "customerId", request.getCustomerId(),
                "totalAmount", request.getTotalAmount(),
                "status", "SAGA_STARTED"
        ));
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderEntity> getOrder(@PathVariable("orderId") String orderId) {
        log.info("Fetching order: {}", orderId);

        return orderService.findById(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<OrderEntity>> getOrdersByCustomer(@PathVariable("customerId") String customerId) {
        log.info("Fetching orders for customer: {}", customerId);

        List<OrderEntity> orders = orderService.findByCustomerId(customerId);
        return ResponseEntity.ok(orders);
    }

    @GetMapping
    public ResponseEntity<List<OrderEntity>> getAllOrders() {
        log.info("Fetching all orders");

        List<OrderEntity> orders = orderService.findAll();
        return ResponseEntity.ok(orders);
    }
}
