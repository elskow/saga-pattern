package com.thesis.orchestration.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ReleaseInventoryCommand;
import com.thesis.common.command.ReserveInventoryCommand;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.orchestration.inventory.model.ProductEntity;
import com.thesis.orchestration.inventory.model.ReservationEntity;
import com.thesis.orchestration.inventory.repository.ProductRepository;
import com.thesis.orchestration.inventory.repository.ReservationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Service layer for inventory operations with proper transactional support.
 * This class is separate from the Kafka listener to ensure Spring AOP proxies work correctly
 * for @Transactional annotations.
 */
@Service
@Slf4j
public class InventoryService {

    private final ReservationRepository reservationRepository;
    private final ProductRepository productRepository;
    private final SagaMetricsHelper metricsHelper;

    // Thesis testing - artificial delay configuration
    @Value("${app.artificial-delay.enabled:false}")
    private boolean artificialDelayEnabled;

    @Value("${app.artificial-delay.duration-ms:0}")
    private long artificialDelayMs;

    public InventoryService(ReservationRepository reservationRepository,
                            ProductRepository productRepository,
                            MeterRegistry meterRegistry) {
        this.reservationRepository = reservationRepository;
        this.productRepository = productRepository;
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }

    /**
     * Reserves inventory for an order. Uses pessimistic locking to prevent concurrent modifications.
     * This method must be called from outside this class for @Transactional to work properly.
     *
     * @param command The reservation command containing order and item details
     * @return ReservationResult indicating success/failure and the reservation entity
     */
    @Transactional
    public ReservationResult reserveInventory(ReserveInventoryCommand command) {
        log.info("Reserving inventory {} for order {}",
            command.getReservationId(), command.getOrderId());

        // Thesis testing - artificial delay for timeout scenarios
        if (artificialDelayEnabled && artificialDelayMs > 0) {
            log.warn("Artificial delay enabled: sleeping for {} ms (thesis timeout test)", artificialDelayMs);
            try {
                Thread.sleep(artificialDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Artificial delay interrupted for order: {}", command.getOrderId());
            }
        }

        // Idempotency check: if reservation already exists, return existing status
        Optional<ReservationEntity> existingReservation = reservationRepository.findById(command.getReservationId());
        if (existingReservation.isPresent()) {
            ReservationEntity existing = existingReservation.get();
            if (existing.getStatus() == ReservationEntity.ReservationStatus.RESERVED) {
                log.info("Reservation {} already exists with RESERVED status",
                    command.getReservationId());
                return ReservationResult.alreadyExists(existing);
            } else if (existing.getStatus() == ReservationEntity.ReservationStatus.FAILED) {
                log.info("Reservation {} already exists with FAILED status",
                    command.getReservationId());
                return ReservationResult.failure("Reservation previously failed: " + existing.getFailureReason());
            }
            // RELEASED status - attempt re-reservation
            log.warn("Reservation {} exists with RELEASED status, attempting re-reservation",
                command.getReservationId());
        }

        List<OrderCreatedEvent.OrderItemEvent> items = command.getItems();

        // Batch query all products at once with pessimistic lock to avoid N+1 problem
        List<String> productIds = items.stream()
            .map(OrderCreatedEvent.OrderItemEvent::getProductId)
            .distinct()
            .toList();

        List<ProductEntity> products = productRepository.findAllByIdInForUpdate(productIds);
        Map<String, ProductEntity> productMap = products.stream()
            .collect(Collectors.toMap(ProductEntity::getProductId, p -> p));

        // Check stock availability for all items
        for (OrderCreatedEvent.OrderItemEvent item : items) {
            ProductEntity product = productMap.get(item.getProductId());
            if (product == null) {
                String error = "Product not found: " + item.getProductId();
                saveFailedReservation(command, error);
                return ReservationResult.failure(error);
            }
            int available = product.getQuantity() -
                (product.getReservedQuantity() != null ? product.getReservedQuantity() : 0);
            if (available < item.getQuantity()) {
                String error = "Insufficient stock for product: " + item.getProductId();
                saveFailedReservation(command, error);
                return ReservationResult.failure(error);
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
        String itemsJson = serializeItems(items, command.getOrderId());
        ReservationEntity reservation = ReservationEntity.builder()
            .reservationId(command.getReservationId())
            .orderId(command.getOrderId())
            .itemsJson(itemsJson)
            .status(ReservationEntity.ReservationStatus.RESERVED)
            .reservedAt(Instant.now())
            .build();
        reservationRepository.save(reservation);
        metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_INVENTORY);

        log.info("Inventory {} reserved successfully", command.getReservationId());
        return ReservationResult.success(reservation);
    }

    /**
     * Releases previously reserved inventory for an order.
     *
     * @param command The release command containing reservation details
     * @return true if release was successful, false otherwise
     */
    @Transactional
    public boolean releaseInventory(ReleaseInventoryCommand command) {
        log.info("Releasing inventory {} for order {}", command.getReservationId(), command.getOrderId());

        Optional<ReservationEntity> reservationOpt = reservationRepository.findById(command.getReservationId());
        if (reservationOpt.isEmpty()) {
            log.warn("Reservation not found: {}", command.getReservationId());
            return false;
        }

        ReservationEntity reservation = reservationOpt.get();

        // Idempotency: already released
        if (reservation.getStatus() == ReservationEntity.ReservationStatus.RELEASED) {
            log.info("Reservation {} already released", command.getReservationId());
            return true;
        }

        // Restore stock for reserved items
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
        return true;
    }

    private void saveFailedReservation(ReserveInventoryCommand command, String reason) {
        ReservationEntity reservation = ReservationEntity.builder()
            .reservationId(command.getReservationId())
            .orderId(command.getOrderId())
            .itemsJson(serializeItems(command.getItems(), command.getOrderId()))
            .status(ReservationEntity.ReservationStatus.FAILED)
            .failureReason(reason)
            .build();
        reservationRepository.save(reservation);
        metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_INVENTORY);
    }

    private String serializeItems(List<OrderCreatedEvent.OrderItemEvent> items, String orderId) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(items);
        } catch (Exception e) {
            log.error("Failed to serialize items for order {}: {}", orderId, e.getMessage());
            return "[]";
        }
    }

    private List<OrderCreatedEvent.OrderItemEvent> deserializeItems(String itemsJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(itemsJson,
                mapper.getTypeFactory().constructCollectionType(List.class, OrderCreatedEvent.OrderItemEvent.class));
        } catch (Exception e) {
            log.error("Failed to deserialize items: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Result of an inventory reservation attempt.
     */
    public record ReservationResult(boolean success, String errorMessage, ReservationEntity reservation) {
        public static ReservationResult success(ReservationEntity reservation) {
            return new ReservationResult(true, null, reservation);
        }

        public static ReservationResult failure(String errorMessage) {
            return new ReservationResult(false, errorMessage, null);
        }

        public static ReservationResult alreadyExists(ReservationEntity existing) {
            return new ReservationResult(true, null, existing);
        }
    }
}
