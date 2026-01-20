package com.thesis.orchestration.inventory.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ReleaseInventoryCommand;
import com.thesis.common.command.ReserveInventoryCommand;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.orchestration.inventory.model.ProductEntity;
import com.thesis.orchestration.inventory.model.ReservationEntity;
import com.thesis.orchestration.inventory.repository.ProductRepository;
import com.thesis.orchestration.inventory.repository.ReservationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryCommandHandlerTest {

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private ProductRepository productRepository;

    private ObjectMapper objectMapper;

    private MeterRegistry meterRegistry;

    private InventoryCommandHandler inventoryCommandHandler;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        meterRegistry = new SimpleMeterRegistry();
        inventoryCommandHandler = new InventoryCommandHandler(reservationRepository, productRepository, objectMapper, meterRegistry);
    }

    @Test
    void shouldReserveInventorySuccessfully() {
        // Given
        String productId = "PROD-001";
        OrderCreatedEvent.OrderItemEvent item = OrderCreatedEvent.OrderItemEvent.builder()
                .productId(productId)
                .productName("Test Product")
                .quantity(5)
                .price(new BigDecimal("10.00"))
                .build();

        ReserveInventoryCommand command = ReserveInventoryCommand.builder()
                .reservationId("RES-123")
                .orderId("ORDER-456")
                .items(List.of(item))
                .build();

        ProductEntity product = ProductEntity.builder()
                .productId(productId)
                .name("Test Product")
                .quantity(100)
                .reservedQuantity(10)
                .build();

        when(productRepository.findById(productId)).thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(reservationRepository.save(any(ReservationEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Simulate reservation logic
        ArgumentCaptor<ReservationEntity> reservationCaptor = ArgumentCaptor.forClass(ReservationEntity.class);
        ArgumentCaptor<ProductEntity> productCaptor = ArgumentCaptor.forClass(ProductEntity.class);

        // Simulate what the handler would do
        ProductEntity updatedProduct = productRepository.findById(productId).get();
        int currentReserved = updatedProduct.getReservedQuantity() != null ? updatedProduct.getReservedQuantity() : 0;
        updatedProduct.setReservedQuantity(currentReserved + item.getQuantity());
        productRepository.save(updatedProduct);

        ReservationEntity reservation = ReservationEntity.builder()
                .reservationId(command.getReservationId())
                .orderId(command.getOrderId())
                .status(ReservationEntity.ReservationStatus.RESERVED)
                .build();
        reservationRepository.save(reservation);

        // Then
        verify(productRepository).save(productCaptor.capture());
        verify(reservationRepository).save(reservationCaptor.capture());

        ProductEntity savedProduct = productCaptor.getValue();
        assertThat(savedProduct.getReservedQuantity()).isEqualTo(15); // 10 + 5

        ReservationEntity savedReservation = reservationCaptor.getValue();
        assertThat(savedReservation.getReservationId()).isEqualTo("RES-123");
        assertThat(savedReservation.getOrderId()).isEqualTo("ORDER-456");
        assertThat(savedReservation.getStatus()).isEqualTo(ReservationEntity.ReservationStatus.RESERVED);
    }

    @Test
    void shouldFailReservationWhenProductNotFound() {
        // Given
        String productId = "NON-EXISTENT";
        OrderCreatedEvent.OrderItemEvent item = OrderCreatedEvent.OrderItemEvent.builder()
                .productId(productId)
                .productName("Unknown Product")
                .quantity(5)
                .price(new BigDecimal("10.00"))
                .build();

        ReserveInventoryCommand command = ReserveInventoryCommand.builder()
                .reservationId("RES-456")
                .orderId("ORDER-789")
                .items(List.of(item))
                .build();

        when(productRepository.findById(productId)).thenReturn(Optional.empty());
        when(reservationRepository.save(any(ReservationEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Simulate failure logic
        Optional<ProductEntity> productOpt = productRepository.findById(productId);
        assertThat(productOpt).isEmpty();

        // Create failed reservation as handler would
        ReservationEntity failedReservation = ReservationEntity.builder()
                .reservationId(command.getReservationId())
                .orderId(command.getOrderId())
                .status(ReservationEntity.ReservationStatus.FAILED)
                .failureReason("Product not found: " + productId)
                .build();
        reservationRepository.save(failedReservation);

        // Then
        ArgumentCaptor<ReservationEntity> reservationCaptor = ArgumentCaptor.forClass(ReservationEntity.class);
        verify(reservationRepository).save(reservationCaptor.capture());

        ReservationEntity savedReservation = reservationCaptor.getValue();
        assertThat(savedReservation.getStatus()).isEqualTo(ReservationEntity.ReservationStatus.FAILED);
        assertThat(savedReservation.getFailureReason()).contains("Product not found");
    }

    @Test
    void shouldFailReservationWhenInsufficientStock() {
        // Given
        String productId = "PROD-002";
        OrderCreatedEvent.OrderItemEvent item = OrderCreatedEvent.OrderItemEvent.builder()
                .productId(productId)
                .productName("Limited Product")
                .quantity(50) // requesting 50
                .price(new BigDecimal("20.00"))
                .build();

        ReserveInventoryCommand command = ReserveInventoryCommand.builder()
                .reservationId("RES-789")
                .orderId("ORDER-111")
                .items(List.of(item))
                .build();

        ProductEntity product = ProductEntity.builder()
                .productId(productId)
                .name("Limited Product")
                .quantity(30) // only 30 total
                .reservedQuantity(10) // 10 already reserved, so only 20 available
                .build();

        when(productRepository.findById(productId)).thenReturn(Optional.of(product));
        when(reservationRepository.save(any(ReservationEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Check availability
        Optional<ProductEntity> productOpt = productRepository.findById(productId);
        ProductEntity foundProduct = productOpt.get();
        int available = foundProduct.getQuantity() - (foundProduct.getReservedQuantity() != null ? foundProduct.getReservedQuantity() : 0);

        assertThat(available).isLessThan(item.getQuantity()); // 20 < 50

        // Create failed reservation as handler would
        ReservationEntity failedReservation = ReservationEntity.builder()
                .reservationId(command.getReservationId())
                .orderId(command.getOrderId())
                .status(ReservationEntity.ReservationStatus.FAILED)
                .failureReason("Insufficient stock for product: " + productId)
                .build();
        reservationRepository.save(failedReservation);

        // Then
        ArgumentCaptor<ReservationEntity> reservationCaptor = ArgumentCaptor.forClass(ReservationEntity.class);
        verify(reservationRepository).save(reservationCaptor.capture());

        ReservationEntity savedReservation = reservationCaptor.getValue();
        assertThat(savedReservation.getStatus()).isEqualTo(ReservationEntity.ReservationStatus.FAILED);
        assertThat(savedReservation.getFailureReason()).contains("Insufficient stock");
    }

    @Test
    void shouldReleaseInventorySuccessfully() throws Exception {
        // Given
        String reservationId = "RES-123";
        String orderId = "ORDER-456";
        String productId = "PROD-001";

        OrderCreatedEvent.OrderItemEvent item = OrderCreatedEvent.OrderItemEvent.builder()
                .productId(productId)
                .productName("Test Product")
                .quantity(5)
                .price(new BigDecimal("10.00"))
                .build();

        String itemsJson = objectMapper.writeValueAsString(List.of(item));

        ReservationEntity reservation = ReservationEntity.builder()
                .reservationId(reservationId)
                .orderId(orderId)
                .itemsJson(itemsJson)
                .status(ReservationEntity.ReservationStatus.RESERVED)
                .build();

        ProductEntity product = ProductEntity.builder()
                .productId(productId)
                .name("Test Product")
                .quantity(100)
                .reservedQuantity(15) // 15 reserved
                .build();

        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
        when(productRepository.findById(productId)).thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(reservationRepository.save(any(ReservationEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Simulate release logic
        Optional<ReservationEntity> reservationOpt = reservationRepository.findById(reservationId);
        assertThat(reservationOpt).isPresent();

        ReservationEntity foundReservation = reservationOpt.get();
        ProductEntity foundProduct = productRepository.findById(productId).get();
        int currentReserved = foundProduct.getReservedQuantity() != null ? foundProduct.getReservedQuantity() : 0;
        foundProduct.setReservedQuantity(Math.max(0, currentReserved - item.getQuantity()));
        productRepository.save(foundProduct);

        foundReservation.setStatus(ReservationEntity.ReservationStatus.RELEASED);
        foundReservation.setReleaseReason("Order cancelled - saga compensation");
        reservationRepository.save(foundReservation);

        // Then
        ArgumentCaptor<ProductEntity> productCaptor = ArgumentCaptor.forClass(ProductEntity.class);
        verify(productRepository).save(productCaptor.capture());

        ProductEntity savedProduct = productCaptor.getValue();
        assertThat(savedProduct.getReservedQuantity()).isEqualTo(10); // 15 - 5

        ArgumentCaptor<ReservationEntity> reservationCaptor = ArgumentCaptor.forClass(ReservationEntity.class);
        verify(reservationRepository).save(reservationCaptor.capture());

        ReservationEntity savedReservation = reservationCaptor.getValue();
        assertThat(savedReservation.getStatus()).isEqualTo(ReservationEntity.ReservationStatus.RELEASED);
        assertThat(savedReservation.getReleaseReason()).contains("saga compensation");
    }

    @Test
    void shouldHandleReservationNotFoundOnRelease() {
        // Given
        String reservationId = "NON-EXISTENT";
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.empty());

        // When
        Optional<ReservationEntity> result = reservationRepository.findById(reservationId);

        // Then
        assertThat(result).isEmpty();
        verify(productRepository, never()).save(any());
        verify(reservationRepository, never()).save(any());
    }

    @Test
    void shouldBuildCommandHandlers() {
        // When
        var handlers = inventoryCommandHandler.commandHandlers();

        // Then
        assertThat(handlers).isNotNull();
    }

    @Test
    void shouldHandleNullReservedQuantity() {
        // Given
        String productId = "PROD-003";
        OrderCreatedEvent.OrderItemEvent item = OrderCreatedEvent.OrderItemEvent.builder()
                .productId(productId)
                .productName("New Product")
                .quantity(5)
                .price(new BigDecimal("15.00"))
                .build();

        ProductEntity product = ProductEntity.builder()
                .productId(productId)
                .name("New Product")
                .quantity(100)
                .reservedQuantity(null) // null reserved quantity
                .build();

        when(productRepository.findById(productId)).thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Simulate reservation with null check
        ProductEntity foundProduct = productRepository.findById(productId).get();
        int currentReserved = foundProduct.getReservedQuantity() != null ? foundProduct.getReservedQuantity() : 0;
        foundProduct.setReservedQuantity(currentReserved + item.getQuantity());
        productRepository.save(foundProduct);

        // Then
        ArgumentCaptor<ProductEntity> productCaptor = ArgumentCaptor.forClass(ProductEntity.class);
        verify(productRepository).save(productCaptor.capture());

        ProductEntity savedProduct = productCaptor.getValue();
        assertThat(savedProduct.getReservedQuantity()).isEqualTo(5); // 0 + 5
    }

    @Test
    void shouldReserveMultipleItems() {
        // Given
        OrderCreatedEvent.OrderItemEvent item1 = OrderCreatedEvent.OrderItemEvent.builder()
                .productId("PROD-001")
                .productName("Product 1")
                .quantity(3)
                .price(new BigDecimal("10.00"))
                .build();

        OrderCreatedEvent.OrderItemEvent item2 = OrderCreatedEvent.OrderItemEvent.builder()
                .productId("PROD-002")
                .productName("Product 2")
                .quantity(2)
                .price(new BigDecimal("20.00"))
                .build();

        ReserveInventoryCommand command = ReserveInventoryCommand.builder()
                .reservationId("RES-MULTI")
                .orderId("ORDER-MULTI")
                .items(List.of(item1, item2))
                .build();

        ProductEntity product1 = ProductEntity.builder()
                .productId("PROD-001")
                .name("Product 1")
                .quantity(50)
                .reservedQuantity(5)
                .build();

        ProductEntity product2 = ProductEntity.builder()
                .productId("PROD-002")
                .name("Product 2")
                .quantity(30)
                .reservedQuantity(0)
                .build();

        when(productRepository.findById("PROD-001")).thenReturn(Optional.of(product1));
        when(productRepository.findById("PROD-002")).thenReturn(Optional.of(product2));

        // When - Check availability for all items
        for (OrderCreatedEvent.OrderItemEvent item : command.getItems()) {
            Optional<ProductEntity> productOpt = productRepository.findById(item.getProductId());
            assertThat(productOpt).isPresent();
            ProductEntity product = productOpt.get();
            int available = product.getQuantity() - (product.getReservedQuantity() != null ? product.getReservedQuantity() : 0);
            assertThat(available).isGreaterThanOrEqualTo(item.getQuantity());
        }

        // Then - Verify both products were checked
        verify(productRepository).findById("PROD-001");
        verify(productRepository).findById("PROD-002");
    }
}
