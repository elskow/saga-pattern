package com.thesis.choreography.inventory.service;

import com.thesis.choreography.inventory.kafka.InventoryEventPublisher;
import com.thesis.choreography.inventory.model.InventoryReservation;
import com.thesis.choreography.inventory.model.Product;
import com.thesis.choreography.inventory.repository.InventoryReservationRepository;
import com.thesis.choreography.inventory.repository.ProductRepository;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private ProductRepository productRepository;

    @Mock
    private InventoryReservationRepository reservationRepository;

    @Mock
    private InventoryEventPublisher eventPublisher;

    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService(
            productRepository,
            reservationRepository,
            eventPublisher,
            new SimpleMeterRegistry()
        );
    }

    @Test
    void shouldReserveInventorySuccessfully() {
        // Given
        PaymentCompletedEvent paymentEvent = new PaymentCompletedEvent(
            "PAY-123",
            "ORDER-456",
            java.math.BigDecimal.valueOf(100),
            "TXN-123",
            Instant.now(),
            "CORR-123",
            Instant.now()
        );

        Product product = Product.builder()
            .productId("PROD-001")
            .productName("Test Product")
            .quantityAvailable(100)
            .quantityReserved(0)
            .build();

        List<InventoryService.ItemToReserve> items = List.of(
            new InventoryService.ItemToReserve("PROD-001", 5)
        );

        when(productRepository.findAllByProductIdIn(List.of("PROD-001"))).thenReturn(List.of(product));

        // When
        inventoryService.reserveInventory(paymentEvent, items);

        // Then
        verify(productRepository).saveAll(anyList());
        verify(reservationRepository).saveAll(anyList());
        verify(eventPublisher).publishInventoryReserved(any(InventoryReservedEvent.class));
    }

    @Test
    void shouldDecrementAvailableQuantityWhenReserving() {
        // Given
        PaymentCompletedEvent paymentEvent = new PaymentCompletedEvent(
            "PAY-123",
            "ORDER-456",
            java.math.BigDecimal.valueOf(100),
            "TXN-123",
            Instant.now(),
            "CORR-123",
            Instant.now()
        );

        Product product = Product.builder()
            .productId("PROD-001")
            .productName("Test Product")
            .quantityAvailable(100)
            .quantityReserved(10)
            .build();

        List<InventoryService.ItemToReserve> items = List.of(
            new InventoryService.ItemToReserve("PROD-001", 5)
        );

        when(productRepository.findAllByProductIdIn(List.of("PROD-001"))).thenReturn(List.of(product));

        // When
        inventoryService.reserveInventory(paymentEvent, items);

        // Then
        ArgumentCaptor<List<Product>> productCaptor = ArgumentCaptor.forClass(List.class);
        verify(productRepository).saveAll(productCaptor.capture());

        Product savedProduct = productCaptor.getValue().getFirst();
        // After reserve(5): quantityAvailable goes from 100 to 95, quantityReserved goes from 10 to 15
        assertThat(savedProduct.getQuantityReserved()).isEqualTo(15);
        assertThat(savedProduct.getQuantityAvailable()).isEqualTo(95);
    }

    @Test
    void shouldPublishFailedEventWhenProductNotFound() {
        // Given
        PaymentCompletedEvent paymentEvent = new PaymentCompletedEvent(
            "PAY-123",
            "ORDER-456",
            java.math.BigDecimal.valueOf(100),
            "TXN-123",
            Instant.now(),
            "CORR-123",
            Instant.now()
        );

        List<InventoryService.ItemToReserve> items = List.of(
            new InventoryService.ItemToReserve("NON-EXISTENT", 5)
        );

        when(productRepository.findAllByProductIdIn(List.of("NON-EXISTENT"))).thenReturn(List.of());

        // When
        inventoryService.reserveInventory(paymentEvent, items);

        // Then
        verify(eventPublisher).publishInventoryReservationFailed(any());
    }

    @Test
    void shouldReleaseInventory() {
        // Given
        String orderId = "ORDER-123";
        InventoryReservation reservation = InventoryReservation.builder()
            .reservationId("RES-001")
            .orderId(orderId)
            .productId("PROD-001")
            .quantity(5)
            .status(InventoryReservation.ReservationStatus.RESERVED)
            .build();

        Product product = Product.builder()
            .productId("PROD-001")
            .productName("Test Product")
            .quantityAvailable(95)
            .quantityReserved(15)
            .build();

        when(reservationRepository.findByOrderId(orderId)).thenReturn(List.of(reservation));
        when(productRepository.findAllByProductIdIn(List.of("PROD-001"))).thenReturn(List.of(product));

        // When
        inventoryService.releaseInventory(orderId);

        // Then
        verify(productRepository).saveAll(anyList());
        verify(reservationRepository).saveAll(anyList());
        verify(eventPublisher).publishInventoryReleased(any());
    }

    @Test
    void shouldUpdateReservationStatusToReleasedWhenReleasing() {
        // Given
        String orderId = "ORDER-123";
        InventoryReservation reservation = InventoryReservation.builder()
            .reservationId("RES-001")
            .orderId(orderId)
            .productId("PROD-001")
            .quantity(5)
            .status(InventoryReservation.ReservationStatus.RESERVED)
            .build();

        Product product = Product.builder()
            .productId("PROD-001")
            .productName("Test Product")
            .quantityAvailable(95)
            .quantityReserved(15)
            .build();

        when(reservationRepository.findByOrderId(orderId)).thenReturn(List.of(reservation));
        when(productRepository.findAllByProductIdIn(List.of("PROD-001"))).thenReturn(List.of(product));

        // When
        inventoryService.releaseInventory(orderId);

        // Then
        assertThat(reservation.getStatus()).isEqualTo(InventoryReservation.ReservationStatus.RELEASED);
    }

    @Test
    void shouldNotReleaseAlreadyReleasedReservation() {
        // Given
        String orderId = "ORDER-123";
        InventoryReservation reservation = InventoryReservation.builder()
            .reservationId("RES-001")
            .orderId(orderId)
            .productId("PROD-001")
            .quantity(5)
            .status(InventoryReservation.ReservationStatus.RELEASED) // Already released
            .build();

        when(reservationRepository.findByOrderId(orderId)).thenReturn(List.of(reservation));

        // When
        inventoryService.releaseInventory(orderId);

        // Then
        verify(productRepository, never()).save(any(Product.class));
        verify(reservationRepository, never()).save(any(InventoryReservation.class));
    }

    @Test
    void shouldPublishFailedEventWhenInsufficientStock() {
        // Given
        PaymentCompletedEvent paymentEvent = new PaymentCompletedEvent(
            "PAY-123",
            "ORDER-456",
            java.math.BigDecimal.valueOf(100),
            "TXN-123",
            Instant.now(),
            "CORR-123",
            Instant.now()
        );

        Product product = Product.builder()
            .productId("PROD-001")
            .productName("Low Stock Product")
            .quantityAvailable(3) // Only 3 available
            .quantityReserved(0)
            .build();

        List<InventoryService.ItemToReserve> items = List.of(
            new InventoryService.ItemToReserve("PROD-001", 10) // Requesting 10
        );

        when(productRepository.findAllByProductIdIn(List.of("PROD-001"))).thenReturn(List.of(product));

        // When
        inventoryService.reserveInventory(paymentEvent, items);

        // Then
        verify(eventPublisher).publishInventoryReservationFailed(any());
    }
}
