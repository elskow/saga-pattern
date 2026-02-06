package com.thesis.orchestration.inventory.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ReleaseInventoryCommand;
import com.thesis.common.command.ReserveInventoryCommand;
import com.thesis.common.replies.InventoryFailedReply;
import com.thesis.common.replies.InventoryReleasedReply;
import com.thesis.common.replies.InventoryReservedReply;
import com.thesis.orchestration.inventory.service.InventoryService;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import com.thesis.saga.participant.AbstractSagaParticipant;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InventorySagaParticipant extends AbstractSagaParticipant {

    private static final String COMMAND_TOPIC = "orchestration.inventory.commands";
    private static final String REPLY_TOPIC = "orchestration.inventory.replies";

    private final InventoryService inventoryService;

    public InventorySagaParticipant(
        KafkaTemplate<String, Object> kafkaTemplate,
        ObjectMapper objectMapper,
        Validator validator,
        SagaMetricsRecorder metricsRecorder,
        InventoryService inventoryService) {
        super(kafkaTemplate, objectMapper, validator, metricsRecorder, "inventory-service", REPLY_TOPIC);
        this.inventoryService = inventoryService;

        registerHandler("RESERVE_INVENTORY", ReserveInventoryCommand.class, this::reserveInventory);
        registerHandler("RELEASE_INVENTORY", ReleaseInventoryCommand.class, this::releaseInventory);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "inventory-service")
    public void handleCommand(String message) {
        super.handleCommand(message);
    }

    private Object reserveInventory(ReserveInventoryCommand command) {
        log.debug("Reserving inventory {} for order {}", command.reservationId(), command.orderId());

        var result = inventoryService.reserveInventory(command);

        if (result.success()) {
            log.debug("Inventory {} reserved successfully", command.reservationId());
            return InventoryReservedReply.of(command.reservationId(), command.orderId());
        } else {
            log.warn("Inventory reservation failed: {}", result.errorMessage());
            return InventoryFailedReply.of(command.reservationId(), command.orderId(), result.errorMessage());
        }
    }

    private Object releaseInventory(ReleaseInventoryCommand command) {
        log.debug("Releasing inventory {} for order {}", command.reservationId(), command.orderId());

        boolean success = inventoryService.releaseInventory(command);

        return success
            ? InventoryReleasedReply.success(command.reservationId(), command.orderId())
            : InventoryReleasedReply.failure(command.reservationId(), command.orderId(), "Release failed");
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
            case ReserveInventoryCommand cmd -> InventoryFailedReply.of(cmd.reservationId(), cmd.orderId(), reason);
            case ReleaseInventoryCommand cmd -> InventoryReleasedReply.failure(cmd.reservationId(), cmd.orderId(), reason);
            default -> null;
        };
    }
}
