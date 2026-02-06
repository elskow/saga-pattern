package com.thesis.choreography.order.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.choreography.order.service.OrderService;
import com.thesis.common.dto.CreateOrderRequest;
import com.thesis.common.dto.OrderResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderService orderService;

    @Test
    void shouldCreateOrder() throws Exception {
        // Given
        CreateOrderRequest request = createOrderRequest();
        OrderResponse response = new OrderResponse(
                "ORDER-123",
                "CUST-001",
                "PENDING",
                List.of(),
                new BigDecimal("99.98"),
                Instant.now(),
                null);

        when(orderService.createOrder(any(CreateOrderRequest.class))).thenReturn(response);

        // When/Then
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value("ORDER-123"))
                .andExpect(jsonPath("$.customerId").value("CUST-001"))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void shouldGetOrderById() throws Exception {
        // Given
        String orderId = "ORDER-123";
        OrderResponse response = new OrderResponse(
                orderId,
                "CUST-001",
                "COMPLETED",
                List.of(),
                new BigDecimal("99.98"),
                Instant.now(),
                null);

        when(orderService.getOrder(orderId)).thenReturn(response);

        // When/Then
        mockMvc.perform(get("/api/orders/{orderId}", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(orderId))
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    @Test
    void shouldGetOrdersByCustomer() throws Exception {
        // Given
        String customerId = "CUST-001";
        List<OrderResponse> orders = List.of(
                new OrderResponse("ORDER-1", customerId, "PENDING", List.of(), null, null, null),
                new OrderResponse("ORDER-2", customerId, "COMPLETED", List.of(), null, null, null)
        );

        when(orderService.getOrdersByCustomer(customerId)).thenReturn(orders);

        // When/Then
        mockMvc.perform(get("/api/orders/customer/{customerId}", customerId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].orderId").value("ORDER-1"))
                .andExpect(jsonPath("$[1].orderId").value("ORDER-2"));
    }

    @Test
    void shouldReturnEmptyListWhenNoOrdersForCustomer() throws Exception {
        // Given
        String customerId = "CUST-999";
        when(orderService.getOrdersByCustomer(customerId)).thenReturn(List.of());

        // When/Then
        mockMvc.perform(get("/api/orders/customer/{customerId}", customerId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }

    private CreateOrderRequest createOrderRequest() {
        var item = new CreateOrderRequest.OrderItemRequest(
                "PROD-001",
                "Test Product",
                2,
                new BigDecimal("49.99"));

        return new CreateOrderRequest(
                "CUST-001",
                "123 Main Street",
                List.of(item));
    }
}
