package com.thesis.orchestration.inventory.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ReleaseInventoryCommand;
import com.thesis.common.command.ReserveInventoryCommand;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.InventoryFailedReply;
import com.thesis.common.replies.InventoryReservedReply;
import com.thesis.orchestration.inventory.model.ProductEntity;
import com.thesis.orchestration.inventory.model.ReservationEntity;
import com.thesis.orchestration.inventory.repository.ProductRepository;
import com.thesis.orchestration.inventory.repository.ReservationRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

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
    private final SagaMetricsHelper metricsHelper;

    public InventoryCommandListener(ReservationRepository reservationRepository,
                                     ProductRepository productRepository,
                                     KafkaTemplate<String, Object> kafkaTemplate,
                                     ObjectMapper objectMapper,
                                     MeterRegistry meterRegistry) {
        this.reservationRepository = reservationRepository;
        this.productRepository = productRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
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
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "inventory-service")
    public void handleCommand(String message) {
        try {
            // Try to parse as ReserveInventoryCommand first
            if (message.contains("items") && message.contains("reservationId")) {
                ReserveInventoryCommand command = objectMapper.readValue(message, ReserveInventoryCommand.class);
                if (command.getItems() != null && !command.getItems().isEmpty()) {
                    handleReserveInventory(command);
                    return;
                }
            }

            // Try to parse as ReleaseInventoryCommand
            if (message.contains("reservationId") && message.contains("orderId")) {
                ReleaseInventoryCommand command = objectMapper.readValue(message, ReleaseInventoryCommand.class);
                handleReleaseInventory(command);
            }
        } catch (Exception e) {
            log.error("Failed to process command: {}", message, e);
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

            // Check stock availability for all items
            for (OrderCreatedEvent.OrderItemEvent item : items) {
                Optional<ProductEntity> productOpt = productRepository.findById(item.getProductId());
                if (productOpt.isEmpty()) {
                    sendFailureResponse(command, "Product not found: " + item.getProductId());
                    return;
                }
                ProductEntity product = productOpt.get();
                int available = product.getQuantity() - (product.getReservedQuantity() != null ? product.getReservedQuantity() : 0);
                if (available < item.getQuantity()) {
                    sendFailureResponse(command, "Insufficient stock for product: " + item.getProductId());
                    return;
                }
            }

            // Reserve stock for all items
            for (OrderCreatedEvent.OrderItemEvent item : items) {
                ProductEntity product = productRepository.findById(item.getProductId()).get();
                int currentReserved = product.getReservedQuantity() != null ? product.getReservedQuantity() : 0;
                product.setReservedQuantity(currentReserved + item.getQuantity());
                productRepository.save(product);
                metricsHelper.recordDbUpdate(command.getOrderId(), SagaMetrics.ENTITY_INVENTORY);
            }

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

            kafkaTemplate.send(REPLY_TOPIC, command.getOrderId(), reply);
            metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
            log.info("Inventory {} reserved successfully", command.getReservationId());
        });
    }

    @Transactional
    @Observed(name = "inventory.release", contextualName = "release-inventory")
    protected void handleReleaseInventory(ReleaseInventoryCommand command) {
        long startTime = System.currentTimeMillis();
        log.info("Releasing inventory {} for order {}", command.getReservationId(), command.getOrderId());

        metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

        reservationRepository.findById(command.getReservationId()).ifPresent(reservation -> {
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
        });

        compensationInventoryCounter.increment();
        compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
        metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
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

        kafkaTemplate.send(REPLY_TOPIC, command.getOrderId(), reply);
        metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
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
}
