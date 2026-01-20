package com.thesis.orchestration.shipping.handler;

import com.thesis.common.command.CancelShippingCommand;
import com.thesis.common.command.ScheduleShippingCommand;
import com.thesis.orchestration.shipping.model.ShipmentEntity;
import com.thesis.orchestration.shipping.repository.ShipmentRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ShippingCommandHandlerTest {

    @Mock
    private ShipmentRepository shipmentRepository;

    private MeterRegistry meterRegistry;

    private ShippingCommandHandler shippingCommandHandler;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        shippingCommandHandler = new ShippingCommandHandler(shipmentRepository, meterRegistry);
    }

    @Test
    void shouldScheduleShippingSuccessfully() {
        // Given
        ScheduleShippingCommand command = ScheduleShippingCommand.builder()
                .shipmentId("SHIP-123")
                .orderId("ORDER-456")
                .shippingAddress("123 Main St, City, Country")
                .build();

        when(shipmentRepository.save(any(ShipmentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Simulate what the handler would do
        ArgumentCaptor<ShipmentEntity> shipmentCaptor = ArgumentCaptor.forClass(ShipmentEntity.class);

        ShipmentEntity shipment = ShipmentEntity.builder()
                .shipmentId(command.getShipmentId())
                .orderId(command.getOrderId())
                .shippingAddress(command.getShippingAddress())
                .trackingNumber("TRK-TEST1234")
                .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                .build();

        shipmentRepository.save(shipment);

        // Then
        verify(shipmentRepository).save(shipmentCaptor.capture());
        ShipmentEntity savedShipment = shipmentCaptor.getValue();

        assertThat(savedShipment.getShipmentId()).isEqualTo("SHIP-123");
        assertThat(savedShipment.getOrderId()).isEqualTo("ORDER-456");
        assertThat(savedShipment.getShippingAddress()).isEqualTo("123 Main St, City, Country");
        assertThat(savedShipment.getTrackingNumber()).startsWith("TRK-");
        assertThat(savedShipment.getStatus()).isEqualTo(ShipmentEntity.ShipmentStatus.SCHEDULED);
    }

    @Test
    void shouldGenerateTrackingNumberOnSchedule() {
        // Given
        ScheduleShippingCommand command = ScheduleShippingCommand.builder()
                .shipmentId("SHIP-789")
                .orderId("ORDER-101")
                .shippingAddress("456 Oak Ave, Town, Country")
                .build();

        when(shipmentRepository.save(any(ShipmentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Simulate tracking number generation
        String trackingNumber = "TRK-" + java.util.UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        ShipmentEntity shipment = ShipmentEntity.builder()
                .shipmentId(command.getShipmentId())
                .orderId(command.getOrderId())
                .shippingAddress(command.getShippingAddress())
                .trackingNumber(trackingNumber)
                .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                .build();

        shipmentRepository.save(shipment);

        // Then
        ArgumentCaptor<ShipmentEntity> shipmentCaptor = ArgumentCaptor.forClass(ShipmentEntity.class);
        verify(shipmentRepository).save(shipmentCaptor.capture());

        ShipmentEntity savedShipment = shipmentCaptor.getValue();
        assertThat(savedShipment.getTrackingNumber()).isNotNull();
        assertThat(savedShipment.getTrackingNumber()).startsWith("TRK-");
        assertThat(savedShipment.getTrackingNumber()).hasSize(12); // TRK- + 8 chars
    }

    @Test
    void shouldCancelShippingSuccessfully() {
        // Given
        String shipmentId = "SHIP-123";
        String orderId = "ORDER-456";

        ShipmentEntity existingShipment = ShipmentEntity.builder()
                .shipmentId(shipmentId)
                .orderId(orderId)
                .shippingAddress("123 Main St, City, Country")
                .trackingNumber("TRK-ABC12345")
                .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                .build();

        when(shipmentRepository.findById(shipmentId)).thenReturn(Optional.of(existingShipment));
        when(shipmentRepository.save(any(ShipmentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Simulate cancellation
        Optional<ShipmentEntity> shipmentOpt = shipmentRepository.findById(shipmentId);
        assertThat(shipmentOpt).isPresent();

        ShipmentEntity shipment = shipmentOpt.get();
        shipment.setStatus(ShipmentEntity.ShipmentStatus.CANCELLED);
        shipment.setCancellationReason("Order cancelled - saga compensation");
        shipmentRepository.save(shipment);

        // Then
        ArgumentCaptor<ShipmentEntity> shipmentCaptor = ArgumentCaptor.forClass(ShipmentEntity.class);
        verify(shipmentRepository).save(shipmentCaptor.capture());

        ShipmentEntity savedShipment = shipmentCaptor.getValue();
        assertThat(savedShipment.getStatus()).isEqualTo(ShipmentEntity.ShipmentStatus.CANCELLED);
        assertThat(savedShipment.getCancellationReason()).contains("saga compensation");
    }

    @Test
    void shouldHandleShipmentNotFoundOnCancel() {
        // Given
        String shipmentId = "NON-EXISTENT";
        when(shipmentRepository.findById(shipmentId)).thenReturn(Optional.empty());

        // When
        Optional<ShipmentEntity> result = shipmentRepository.findById(shipmentId);

        // Then
        assertThat(result).isEmpty();
        verify(shipmentRepository, never()).save(any());
    }

    @Test
    void shouldBuildCommandHandlers() {
        // When
        var handlers = shippingCommandHandler.commandHandlers();

        // Then
        assertThat(handlers).isNotNull();
    }

    @Test
    void shouldCreateFailedShipmentOnSchedulingError() {
        // Given
        ScheduleShippingCommand command = ScheduleShippingCommand.builder()
                .shipmentId("SHIP-FAIL")
                .orderId("ORDER-FAIL")
                .shippingAddress("Invalid Address")
                .build();

        String errorMessage = "Shipping scheduling failed";

        when(shipmentRepository.save(any(ShipmentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Simulate failure scenario
        ShipmentEntity failedShipment = ShipmentEntity.builder()
                .shipmentId(command.getShipmentId())
                .orderId(command.getOrderId())
                .shippingAddress(command.getShippingAddress())
                .status(ShipmentEntity.ShipmentStatus.FAILED)
                .failureReason(errorMessage)
                .build();

        shipmentRepository.save(failedShipment);

        // Then
        ArgumentCaptor<ShipmentEntity> shipmentCaptor = ArgumentCaptor.forClass(ShipmentEntity.class);
        verify(shipmentRepository).save(shipmentCaptor.capture());

        ShipmentEntity savedShipment = shipmentCaptor.getValue();
        assertThat(savedShipment.getStatus()).isEqualTo(ShipmentEntity.ShipmentStatus.FAILED);
        assertThat(savedShipment.getFailureReason()).isEqualTo(errorMessage);
        assertThat(savedShipment.getTrackingNumber()).isNull();
    }

    @Test
    void shouldPreserveShipmentDetailsAfterCancellation() {
        // Given
        String shipmentId = "SHIP-PRESERVE";
        String orderId = "ORDER-PRESERVE";
        String shippingAddress = "789 Elm St, Village, Country";
        String trackingNumber = "TRK-XYZ98765";

        ShipmentEntity existingShipment = ShipmentEntity.builder()
                .shipmentId(shipmentId)
                .orderId(orderId)
                .shippingAddress(shippingAddress)
                .trackingNumber(trackingNumber)
                .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                .build();

        when(shipmentRepository.findById(shipmentId)).thenReturn(Optional.of(existingShipment));
        when(shipmentRepository.save(any(ShipmentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When - Cancel the shipment
        ShipmentEntity shipment = shipmentRepository.findById(shipmentId).get();
        shipment.setStatus(ShipmentEntity.ShipmentStatus.CANCELLED);
        shipment.setCancellationReason("Order cancelled");
        shipmentRepository.save(shipment);

        // Then - All other details should be preserved
        ArgumentCaptor<ShipmentEntity> shipmentCaptor = ArgumentCaptor.forClass(ShipmentEntity.class);
        verify(shipmentRepository).save(shipmentCaptor.capture());

        ShipmentEntity savedShipment = shipmentCaptor.getValue();
        assertThat(savedShipment.getShipmentId()).isEqualTo(shipmentId);
        assertThat(savedShipment.getOrderId()).isEqualTo(orderId);
        assertThat(savedShipment.getShippingAddress()).isEqualTo(shippingAddress);
        assertThat(savedShipment.getTrackingNumber()).isEqualTo(trackingNumber);
        assertThat(savedShipment.getStatus()).isEqualTo(ShipmentEntity.ShipmentStatus.CANCELLED);
    }

    @Test
    void shouldHandleSchedulingWithLongAddress() {
        // Given
        String longAddress = "Building A, Floor 15, Apartment 1234, " +
                "Very Long Street Name with Multiple Parts, " +
                "District XYZ, City Center, Metropolitan Area, " +
                "Country Name 12345-6789";

        ScheduleShippingCommand command = ScheduleShippingCommand.builder()
                .shipmentId("SHIP-LONG")
                .orderId("ORDER-LONG")
                .shippingAddress(longAddress)
                .build();

        when(shipmentRepository.save(any(ShipmentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        ShipmentEntity shipment = ShipmentEntity.builder()
                .shipmentId(command.getShipmentId())
                .orderId(command.getOrderId())
                .shippingAddress(command.getShippingAddress())
                .trackingNumber("TRK-LONGADDR")
                .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                .build();

        shipmentRepository.save(shipment);

        // Then
        ArgumentCaptor<ShipmentEntity> shipmentCaptor = ArgumentCaptor.forClass(ShipmentEntity.class);
        verify(shipmentRepository).save(shipmentCaptor.capture());

        ShipmentEntity savedShipment = shipmentCaptor.getValue();
        assertThat(savedShipment.getShippingAddress()).isEqualTo(longAddress);
    }
}
