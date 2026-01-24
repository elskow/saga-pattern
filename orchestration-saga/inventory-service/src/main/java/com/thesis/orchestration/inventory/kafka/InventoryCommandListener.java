package com.thesis.orchestration.inventory.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ReleaseInventoryCommand;
import com.thesis.common.command.ReserveInventoryCommand;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.InventoryFailedReply;
import com.thesis.common.replies.InventoryReleasedReply;
import com.thesis.common.replies.InventoryReservedReply;
import com.thesis.orchestration.inventory.model.ProductEntity;
import com.thesis.orchestration.inventory.model.ReservationEntity;
import com.thesis.orchestration.inventory.repository.ProductRepository;
import com.thesis.orchestration.inventory.repository.ReservationRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Kafka-based command listener for inventory service.
 * Handles inventory reservation and release commands from the saga orchestrator.
 */
@Component
@Slf4j
public class InventoryCommandListener {

    private static final String COMMAND_TOPIC = "orchestration.inventory.commands";
    private static final String REPLY_TOPIC = "orchestration.inventory.replies";

    private final ReservationRepository reservationRepository;
    private final ProductRepository productRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Counter reservationSuccessCounter;
    private final Counter reservationFailedCounter;
    private final Timer reservationTimer;
    private final Counter compensationInventoryCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final Counter kafkaReplySendFailureCounter;
    private final SagaMetricsHelper metricsHelper;
    private final Validator validator;

    public InventoryCommandListener(ReservationRepository reservationRepository,
                                     ProductRepository productRepository,
                                     KafkaTemplate<String, Object> kafkaTemplate,
                                     ObjectMapper objectMapper,
                                     MeterRegistry meterRegistry,
                                     Validator validator) {
        this.reservationRepository = reservationRepository;
        this.productRepository = productRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.reservationSuccessCounter = meterRegistry.counter(SagaMetrics.INVENTORY_RESERVATIONS_SUCCESS,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.reservationFailedCounter = meterRegistry.counter(SagaMetrics.INVENTORY_RESERVATIONS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.reservationTimer = meterRegistry.timer(SagaMetrics.STEP_INVENTORY_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationInventoryCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_INVENTORY,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationDurationTimer = meterRegistry.timer(SagaMetrics.COMPENSATION_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.sagaStepsExecutedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_EXECUTED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.sagaStepsFailedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.kafkaReplySendFailureCounter = meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT,
                SagaMetrics.TAG_MESSAGE_TYPE, SagaMetrics.TYPE_REPLY,
                SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_FAILURE
        );
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "inventory-service")
    public void handleCommand(String message) {
        String correlationId = UUID.randomUUID().toString();
        try {
            JsonNode node = objectMapper.readTree(message);
            String commandType = node.path("commandType").asText(null);
            if (commandType == null) {
                log.warn("Inventory command missing commandType: {}", message);
                return;
            }

            switch (commandType) {
                case "RESERVE_INVENTORY" -> {
                    ReserveInventoryCommand command = objectMapper.readValue(message, ReserveInventoryCommand.class);
                    if (command.getOrderId() != null) {
                        MDC.put("orderId", command.getOrderId());
                    }
                    MDC.put("correlationId", correlationId);
                    if (validateCommand(command)) {
                        if (command.getItems() != null && !command.getItems().isEmpty()) {
                            handleReserveInventory(command);
                        } else {
                            log.warn("Reserve inventory command has no items: {}", message);
                        }
                    }
                }
                case "RELEASE_INVENTORY" -> {
                    ReleaseInventoryCommand command = objectMapper.readValue(message, ReleaseInventoryCommand.class);
                    if (command.getOrderId() != null) {
                        MDC.put("orderId", command.getOrderId());
                    }
                    MDC.put("correlationId", correlationId);
                    if (validateCommand(command)) {
                        handleReleaseInventory(command);
                    }
                }
                default -> log.warn("Unknown inventory command type: {}", commandType);
            }
        } catch (JsonProcessingException e) {
            log.error("JSON parsing failed for inventory command: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing inventory command: {}", e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    @Transactional
    @Observed(name = "inventory.reserve", contextualName = "reserve-inventory")
    protected void handleReserveInventory(ReserveInventoryCommand command) {
        reservationTimer.record(() -> {
            log.info("Reserving inventory {} for order {}",
                    command.getReservationId(), command.getOrderId());

            metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

            List<OrderCreatedEvent.OrderItemEvent> items = command.getItems();

            // Batch query all products at once to avoid N+1 problem
            List<String> productIds = items.stream()
                    .map(OrderCreatedEvent.OrderItemEvent::getProductId)
                    .distinct()
                    .toList();
            
            List<ProductEntity> products = productRepository.findAllByIdIn(productIds);
            java.util.Map<String, ProductEntity> productMap = products.stream()
                    .collect(java.util.stream.Collectors.toMap(ProductEntity::getProductId, p -> p));

            // Check stock availability for all items
            for (OrderCreatedEvent.OrderItemEvent item : items) {
                ProductEntity product = productMap.get(item.getProductId());
                if (product == null) {
                    sendFailureResponse(command, "Product not found: " + item.getProductId());
                    return;
                }
                int available = product.getQuantity() - (product.getReservedQuantity() != null ? product.getReservedQuantity() : 0);
                if (available < item.getQuantity()) {
                    sendFailureResponse(command, "Insufficient stock for product: " + item.getProductId());
                    return;
                }
            }

            // Reserve stock for all items (batch update)
            for (OrderCreatedEvent.OrderItemEvent item : items) {
                ProductEntity product = productMap.get(item.getProductId());
                int currentReserved = product.getReservedQuantity() != null ? product.getReservedQuantity() : 0;
                product.setReservedQuantity(currentReserved + item.getQuantity());
                metricsHelper.recordDbUpdate(command.getOrderId(), SagaMetrics.ENTITY_INVENTORY);
            }
            productRepository.saveAll(products);

            // Create reservation record
            String itemsJson = serializeItems(items);
            ReservationEntity reservation = ReservationEntity.builder()
                    .reservationId(command.getReservationId())
                    .orderId(command.getOrderId())
                    .itemsJson(itemsJson)
                    .status(ReservationEntity.ReservationStatus.RESERVED)
                    .reservedAt(Instant.now())
                    .build();
            reservationRepository.save(reservation);
            metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_INVENTORY);

            reservationSuccessCounter.increment();
            sagaStepsExecutedCounter.increment();

            InventoryReservedReply reply = InventoryReservedReply.builder()
                    .reservationId(command.getReservationId())
                    .orderId(command.getOrderId())
                    .build();

            sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
            log.info("Inventory {} reserved successfully", command.getReservationId());
        });
    }

    @Transactional
    @Observed(name = "inventory.release", contextualName = "release-inventory")
    protected void handleReleaseInventory(ReleaseInventoryCommand command) {
        long startTime = System.currentTimeMillis();
        log.info("Releasing inventory {} for order {}", command.getReservationId(), command.getOrderId());

        metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

        boolean success = false;
        String reason = null;

        try {
            var reservationOpt = reservationRepository.findById(command.getReservationId());
            if (reservationOpt.isPresent()) {
                var reservation = reservationOpt.get();
                List<OrderCreatedEvent.OrderItemEvent> items = deserializeItems(reservation.getItemsJson());
                if (items != null) {
                    for (OrderCreatedEvent.OrderItemEvent item : items) {
                        productRepository.findById(item.getProductId()).ifPresent(product -> {
                            int currentReserved = product.getReservedQuantity() != null ? product.getReservedQuantity() : 0;
                            product.setReservedQuantity(Math.max(0, currentReserved - item.getQuantity()));
                            productRepository.save(product);
                            metricsHelper.recordDbUpdate(command.getOrderId(), SagaMetrics.ENTITY_INVENTORY);
                        });
                    }
                }

                reservation.setStatus(ReservationEntity.ReservationStatus.RELEASED);
                reservation.setReleasedAt(Instant.now());
                reservation.setReleaseReason("Order cancelled - saga compensation");
                reservationRepository.save(reservation);
                metricsHelper.recordDbUpdate(command.getOrderId(), SagaMetrics.ENTITY_INVENTORY);
                log.info("Inventory {} released successfully", command.getReservationId());
                success = true;
            } else {
                reason = "Reservation not found: " + command.getReservationId();
                log.warn(reason);
            }
        } catch (DataAccessException e) {
            reason = "Database operation failed: " + e.getMessage();
            log.error("Database error while releasing inventory {}: {}", command.getReservationId(), e.getMessage());
        } catch (Exception e) {
            reason = "Release failed: " + e.getMessage();
            log.error("Unexpected error releasing inventory {}: {}", command.getReservationId(), e.getMessage(), e);
        }

        // Send compensation reply
        InventoryReleasedReply reply = InventoryReleasedReply.builder()
                .reservationId(command.getReservationId())
                .orderId(command.getOrderId())
                .success(success)
                .reason(reason)
                .build();
        sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());

        compensationInventoryCounter.increment();
        compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
    }

    /**
     * Sends a reply to Kafka with comprehensive error handling.
     * Uses CompletableFuture callback to handle async results and failures.
     */
    private void sendReplySafely(String topic, String key, Object reply, String orderId) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, reply);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_REPLY);
                    log.debug("Successfully sent reply for order {} to topic {}", orderId, topic);
                } else {
                    kafkaReplySendFailureCounter.increment();
                    log.error("Failed to send reply for order {} to topic {}: {}",
                            orderId, topic, ex.getMessage(), ex);
                    // Note: Replies are typically not retried as they are responses to commands
                    // If reply fails, the orchestrator will timeout and handle accordingly
                }
            });
        } catch (Exception e) {
            kafkaReplySendFailureCounter.increment();
            log.error("Exception while sending reply for order {} to topic {}: {}",
                    orderId, topic, e.getMessage(), e);
        }
    }

    private void sendFailureResponse(ReserveInventoryCommand command, String reason) {
        ReservationEntity reservation = ReservationEntity.builder()
                .reservationId(command.getReservationId())
                .orderId(command.getOrderId())
                .itemsJson(serializeItems(command.getItems()))
                .status(ReservationEntity.ReservationStatus.FAILED)
                .failureReason(reason)
                .build();
        reservationRepository.save(reservation);
        metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_INVENTORY);

        reservationFailedCounter.increment();
        sagaStepsFailedCounter.increment();

        InventoryFailedReply reply = InventoryFailedReply.builder()
                .reservationId(command.getReservationId())
                .orderId(command.getOrderId())
                .reason(reason)
                .build();

        sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
        log.error("Inventory {} reservation failed: {}", command.getReservationId(), reason);
    }

    private String serializeItems(List<OrderCreatedEvent.OrderItemEvent> items) {
        try {
            return objectMapper.writeValueAsString(items);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize items: {}", e.getMessage());
            return "[]";
        }
    }

    private List<OrderCreatedEvent.OrderItemEvent> deserializeItems(String itemsJson) {
        try {
            return objectMapper.readValue(itemsJson,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, OrderCreatedEvent.OrderItemEvent.class));
        } catch (Exception e) {
            log.error("Failed to deserialize items: {}", e.getMessage());
            return java.util.Collections.emptyList();
        }
    }

    private <T> boolean validateCommand(T command) {
        Set<ConstraintViolation<T>> violations = validator.validate(command);
        if (!violations.isEmpty()) {
            log.error("Command validation failed: {}", violations.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("Unknown validation error"));
            return false;
        }
        return true;
    }
}
