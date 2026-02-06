package com.thesis.orchestration.shipping.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.CancelShippingCommand;
import com.thesis.common.command.ScheduleShippingCommand;
import com.thesis.common.replies.ShippingCancelledReply;
import com.thesis.common.replies.ShippingFailedReply;
import com.thesis.common.replies.ShippingScheduledReply;
import com.thesis.orchestration.shipping.model.ShipmentEntity;
import com.thesis.orchestration.shipping.repository.ShipmentRepository;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import com.thesis.saga.participant.AbstractSagaParticipant;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Component
@Slf4j
public class ShippingSagaParticipant extends AbstractSagaParticipant {

    private static final String COMMAND_TOPIC = "orchestration.shipping.commands";
    private static final String REPLY_TOPIC = "orchestration.shipping.replies";

    private final ShipmentRepository shipmentRepository;

    public ShippingSagaParticipant(
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper,
            Validator validator,
            SagaMetricsRecorder metricsRecorder,
            ShipmentRepository shipmentRepository) {
        super(kafkaTemplate, objectMapper, validator, metricsRecorder, "shipping-service", REPLY_TOPIC);
        this.shipmentRepository = shipmentRepository;

        registerHandler("SCHEDULE_SHIPPING", ScheduleShippingCommand.class, this::scheduleShipping);
        registerHandler("CANCEL_SHIPPING", CancelShippingCommand.class, this::cancelShipping);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "shipping-service")
    public void handleCommand(String message) {
        super.handleCommand(message);
    }

    @Transactional
    protected Object scheduleShipping(ScheduleShippingCommand command) {
        log.debug("Scheduling shipping {} for order {}", command.shippingId(), command.orderId());

        var existingReplyOpt = shipmentRepository.findById(command.shippingId())
            .flatMap(existing -> {
                log.debug("Shipment {} exists with status {}", command.shippingId(), existing.getStatus());
                return switch (existing.getStatus()) {
                    case SCHEDULED -> Optional.of((Object) ShippingScheduledReply.of(command.shippingId(), command.orderId()));
                    case FAILED -> Optional.of(ShippingFailedReply.of(command.shippingId(), command.orderId(),
                            "Shipment previously failed: %s".formatted(existing.getFailureReason())));
                    default -> Optional.empty();
                };
            });
        if (existingReplyOpt.isPresent()) return existingReplyOpt.get();

        shipmentRepository.save(ShipmentEntity.builder()
                .shipmentId(command.shippingId())
                .orderId(command.orderId())
                .shippingAddress(command.shippingAddress())
                .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                .scheduledAt(Instant.now())
                .build());
        log.debug("Shipping {} scheduled successfully", command.shippingId());

        return ShippingScheduledReply.of(command.shippingId(), command.orderId());
    }

    @Transactional
    protected Object cancelShipping(CancelShippingCommand command) {
        log.debug("Cancelling shipping {} for order {}", command.shippingId(), command.orderId());

        return shipmentRepository.findById(command.shippingId())
            .map(shipment -> {
                if (shipment.getStatus() == ShipmentEntity.ShipmentStatus.CANCELLED) {
                    log.debug("Shipment {} already cancelled", command.shippingId());
                    return buildCancelReply(command);
                }
                shipment.setStatus(ShipmentEntity.ShipmentStatus.CANCELLED);
                shipment.setCancelledAt(Instant.now());
                shipment.setCancellationReason("Saga compensation");
                shipmentRepository.save(shipment);
                log.debug("Shipping {} cancelled successfully", command.shippingId());
                return buildCancelReply(command);
            })
            .orElseGet(() -> {
                log.warn("Shipment {} not found for cancellation", command.shippingId());
                return buildCancelReply(command);
            });
    }

    private ShippingCancelledReply buildCancelReply(CancelShippingCommand cmd) {
        return ShippingCancelledReply.success(cmd.shippingId(), cmd.orderId());
    }

    @Override
    protected Object createValidationFailureReply(Object command, String commandType, String error) {
        return createFailureReply(command, "Validation failed: %s".formatted(error));
    }

    @Override
    protected Object createErrorReply(Object command, String commandType, String error) {
        return createFailureReply(command, "Processing error: %s".formatted(error));
    }

    private Object createFailureReply(Object command, String reason) {
        return switch (command) {
            case ScheduleShippingCommand cmd -> ShippingFailedReply.of(cmd.shippingId(), cmd.orderId(), reason);
            case CancelShippingCommand cmd -> ShippingCancelledReply.failure(cmd.shippingId(), cmd.orderId(), reason);
            default -> null;
        };
    }
}
