package com.thesis.choreography.shipping.service;

import com.thesis.choreography.shipping.kafka.ShippingEventPublisher;
import com.thesis.choreography.shipping.model.Shipment;
import com.thesis.choreography.shipping.repository.ShipmentRepository;
import com.thesis.common.config.ShippingProperties;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.ShippingScheduledEvent;
import com.thesis.common.exception.ShipmentNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ShippingServiceTest {

    @Mock
    private ShipmentRepository shipmentRepository;

    @Mock
    private ShippingEventPublisher eventPublisher;

    private ShippingService shippingService;

    @BeforeEach
    void setUp() {
        ShippingProperties shippingProperties = new ShippingProperties();
        shippingService = new ShippingService(shipmentRepository, eventPublisher, shippingProperties, new SimpleMeterRegistry());
    }

    @Test
    void shouldScheduleShippingSuccessfully() {
        // Given
        InventoryReservedEvent inventoryEvent = new InventoryReservedEvent(
            "RES-123",
            "ORDER-456",
            List.of(),
            Instant.now(),
            "CORR-123",
            Instant.now()
        );

        String shippingAddress = "123 Main Street, City, Country";

        when(shipmentRepository.save(any(Shipment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        shippingService.scheduleShipping(inventoryEvent, shippingAddress);

        // Then - Shipment is saved twice (once PENDING, once SCHEDULED)
        verify(shipmentRepository, times(2)).save(any(Shipment.class));
        verify(eventPublisher).publishShippingScheduled(any(ShippingScheduledEvent.class));
    }

    @Test
    void shouldCreateShipmentWithCorrectData() {
        // Given
        InventoryReservedEvent inventoryEvent = new InventoryReservedEvent(
            "RES-123",
            "ORDER-456",
            List.of(),
            Instant.now(),
            "CORR-123",
            Instant.now()
        );

        String shippingAddress = "123 Main Street";

        // Capture the status at each save call since the same object is mutated
        List<Shipment.ShippingStatus> capturedStatuses = new ArrayList<>();
        ArgumentCaptor<Shipment> shipmentCaptor = ArgumentCaptor.forClass(Shipment.class);
        when(shipmentRepository.save(any(Shipment.class))).thenAnswer(invocation -> {
            Shipment shipment = invocation.getArgument(0);
            capturedStatuses.add(shipment.getStatus());
            return shipment;
        });

        // When
        shippingService.scheduleShipping(inventoryEvent, shippingAddress);

        // Then - Verify final state (after both saves)
        verify(shipmentRepository, times(2)).save(shipmentCaptor.capture());
        Shipment savedShipment = shipmentCaptor.getValue(); // Gets the last captured value

        assertThat(savedShipment.getOrderId()).isEqualTo("ORDER-456");
        assertThat(savedShipment.getShippingAddress()).isEqualTo(shippingAddress);
        // First save is PENDING, second save is SCHEDULED
        assertThat(capturedStatuses).hasSize(2);
        assertThat(capturedStatuses.getFirst()).isEqualTo(Shipment.ShippingStatus.PENDING);
        assertThat(capturedStatuses.get(1)).isEqualTo(Shipment.ShippingStatus.SCHEDULED);
    }

    @Test
    void shouldCancelShippingSuccessfully() {
        // Given
        String orderId = "ORDER-123";
        Shipment existingShipment = Shipment.builder()
            .shippingId("SHIP-456")
            .orderId(orderId)
            .status(Shipment.ShippingStatus.SCHEDULED)
            .build();

        when(shipmentRepository.findByOrderId(orderId)).thenReturn(Optional.of(existingShipment));

        // When
        shippingService.cancelShipping(orderId);

        // Then
        verify(shipmentRepository).save(any(Shipment.class));
        assertThat(existingShipment.getStatus()).isEqualTo(Shipment.ShippingStatus.CANCELLED);
    }

    @Test
    void shouldThrowExceptionWhenShipmentNotFoundForCancel() {
        // Given
        String orderId = "ORDER-999";
        when(shipmentRepository.findByOrderId(orderId)).thenReturn(Optional.empty());

        // When & Then
        assertThatThrownBy(() -> shippingService.cancelShipping(orderId))
            .isInstanceOf(ShipmentNotFoundException.class)
            .hasMessageContaining(orderId);

        verify(shipmentRepository, never()).save(any());
    }

    @Test
    void shouldGenerateTrackingNumber() {
        // Given
        InventoryReservedEvent inventoryEvent = new InventoryReservedEvent(
            "RES-123",
            "ORDER-456",
            List.of(),
            Instant.now(),
            "CORR-123",
            Instant.now()
        );

        ArgumentCaptor<Shipment> shipmentCaptor = ArgumentCaptor.forClass(Shipment.class);
        when(shipmentRepository.save(any(Shipment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        shippingService.scheduleShipping(inventoryEvent, "123 Main St");

        // Then - Shipment is saved twice (once PENDING, once SCHEDULED with tracking)
        verify(shipmentRepository, times(2)).save(shipmentCaptor.capture());
        Shipment savedShipment = shipmentCaptor.getValue(); // Gets the last captured value

        assertThat(savedShipment.getTrackingNumber()).isNotNull();
        assertThat(savedShipment.getTrackingNumber()).startsWith("TRK-");
    }
}
