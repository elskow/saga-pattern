package com.thesis.orchestration.shipping.handler;

import com.thesis.common.command.CancelShippingCommand;
import com.thesis.common.command.ScheduleShippingCommand;
import com.thesis.common.replies.ShippingFailedReply;
import com.thesis.common.replies.ShippingScheduledReply;
import com.thesis.orchestration.shipping.model.ShipmentEntity;
import com.thesis.orchestration.shipping.repository.ShipmentRepository;
import io.eventuate.tram.commands.consumer.CommandHandlers;
import io.eventuate.tram.commands.consumer.CommandMessage;
import io.eventuate.tram.messaging.common.Message;
import io.eventuate.tram.sagas.participant.SagaCommandHandlersBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

import static io.eventuate.tram.commands.consumer.CommandHandlerReplyBuilder.withFailure;
import static io.eventuate.tram.commands.consumer.CommandHandlerReplyBuilder.withSuccess;

@Component
@Slf4j
public class ShippingCommandHandler {

    private final ShipmentRepository shipmentRepository;
    private final Counter shippingSuccessCounter;
    private final Counter shippingFailedCounter;
    private final Timer shippingProcessingTimer;

    public ShippingCommandHandler(ShipmentRepository shipmentRepository, MeterRegistry meterRegistry) {
        this.shipmentRepository = shipmentRepository;
        this.shippingSuccessCounter = meterRegistry.counter("shipping.success", "service", "orchestration");
        this.shippingFailedCounter = meterRegistry.counter("shipping.failed", "service", "orchestration");
        this.shippingProcessingTimer = meterRegistry.timer("shipping.processing.time", "service", "orchestration");
    }

    public CommandHandlers commandHandlers() {
        return SagaCommandHandlersBuilder
            .fromChannel("shipping-service")
            .onMessage(ScheduleShippingCommand.class, this::handleScheduleShipping)
            .onMessage(CancelShippingCommand.class, this::handleCancelShipping)
            .build();
    }

    private Message handleScheduleShipping(CommandMessage<ScheduleShippingCommand> cm) {
        return shippingProcessingTimer.record(() -> {
            ScheduleShippingCommand command = cm.getCommand();
            log.info("Scheduling shipping {} for order {} to address: {}",
                command.getShipmentId(), command.getOrderId(), command.getShippingAddress());

            try {
                // Generate tracking number
                String trackingNumber = "TRK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

                // Create shipment entity
                ShipmentEntity shipment = ShipmentEntity.builder()
                    .shipmentId(command.getShipmentId())
                    .orderId(command.getOrderId())
                    .shippingAddress(command.getShippingAddress())
                    .trackingNumber(trackingNumber)
                    .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                    .scheduledAt(Instant.now())
                    .build();
                shipmentRepository.save(shipment);

                shippingSuccessCounter.increment();
                log.info("Shipping {} scheduled successfully with tracking: {}", command.getShipmentId(), trackingNumber);
                return withSuccess(ShippingScheduledReply.builder()
                    .shipmentId(command.getShipmentId())
                    .orderId(command.getOrderId())
                    .build());
            } catch (Exception e) {
                // Create failed shipment record
                ShipmentEntity shipment = ShipmentEntity.builder()
                    .shipmentId(command.getShipmentId())
                    .orderId(command.getOrderId())
                    .shippingAddress(command.getShippingAddress())
                    .status(ShipmentEntity.ShipmentStatus.FAILED)
                    .failureReason(e.getMessage())
                    .build();
                shipmentRepository.save(shipment);

                shippingFailedCounter.increment();
                log.error("Shipping {} scheduling failed: {}", command.getShipmentId(), e.getMessage());
                return withFailure(ShippingFailedReply.builder()
                    .shipmentId(command.getShipmentId())
                    .orderId(command.getOrderId())
                    .reason("Shipping scheduling failed: " + e.getMessage())
                    .build());
            }
        });
    }

    private Message handleCancelShipping(CommandMessage<CancelShippingCommand> cm) {
        CancelShippingCommand command = cm.getCommand();
        log.info("Cancelling shipping {} for order {}", command.getShipmentId(), command.getOrderId());

        shipmentRepository.findById(command.getShipmentId()).ifPresent(shipment -> {
            shipment.setStatus(ShipmentEntity.ShipmentStatus.CANCELLED);
            shipment.setCancelledAt(Instant.now());
            shipment.setCancellationReason("Order cancelled - saga compensation");
            shipmentRepository.save(shipment);
            log.info("Shipping {} cancelled successfully", command.getShipmentId());
        });

        return withSuccess();
    }
}
