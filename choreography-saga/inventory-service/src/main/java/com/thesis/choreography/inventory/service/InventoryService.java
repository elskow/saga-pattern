package com.thesis.choreography.inventory.service;

import com.thesis.choreography.inventory.kafka.InventoryEventPublisher;
import com.thesis.choreography.inventory.model.InventoryReservation;
import com.thesis.choreography.inventory.model.Product;
import com.thesis.choreography.inventory.repository.InventoryReservationRepository;
import com.thesis.choreography.inventory.repository.ProductRepository;
import com.thesis.common.events.InventoryReleasedEvent;
import com.thesis.common.events.InventoryReservationFailedEvent;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.exception.InsufficientStockException;
import com.thesis.common.exception.ProductNotFoundException;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
public class InventoryService {

    private final ProductRepository productRepository;
    private final InventoryReservationRepository reservationRepository;
    private final InventoryEventPublisher eventPublisher;
    private final Counter reservationSuccessCounter;
    private final Counter reservationFailedCounter;
    private final Timer stepInventoryDurationTimer;
    private final Counter compensationInventoryCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    @Getter
    private final SagaMetricsHelper metricsHelper;

    // Thesis testing - artificial delay configuration
    @Value("${app.artificial-delay.enabled:false}")
    private boolean artificialDelayEnabled;

    @Value("${app.artificial-delay.duration-ms:0}")
    private long artificialDelayMs;

    public InventoryService(ProductRepository productRepository,
                            InventoryReservationRepository reservationRepository,
                            InventoryEventPublisher eventPublisher,
                            MeterRegistry meterRegistry) {
        this.productRepository = productRepository;
        this.reservationRepository = reservationRepository;
        this.eventPublisher = eventPublisher;

        this.reservationSuccessCounter = meterRegistry.counter(SagaMetrics.INVENTORY_RESERVATIONS_SUCCESS,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.reservationFailedCounter = meterRegistry.counter(SagaMetrics.INVENTORY_RESERVATIONS_FAILED,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.stepInventoryDurationTimer = meterRegistry.timer(SagaMetrics.STEP_INVENTORY_DURATION,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.compensationInventoryCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_INVENTORY,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.compensationDurationTimer = meterRegistry.timer(SagaMetrics.COMPENSATION_DURATION,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
            SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.sagaStepsExecutedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_EXECUTED,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
            SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.sagaStepsFailedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_FAILED,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
            SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_CHOREOGRAPHY);
    }

    @Transactional
    @Observed(name = "inventory.reserve", contextualName = "reserve-inventory")
    public void reserveInventory(PaymentCompletedEvent paymentEvent, List<ItemToReserve> items) {
        // Input validation
        if (paymentEvent == null) {
            throw new IllegalArgumentException("PaymentCompletedEvent cannot be null");
        }
        if (paymentEvent.getOrderId() == null || paymentEvent.getOrderId().isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }
        if (items == null || items.isEmpty()) {
            throw new IllegalArgumentException("Items list cannot be null or empty");
        }

        String orderId = paymentEvent.getOrderId();
        try {
            MDC.put("orderId", orderId);
            stepInventoryDurationTimer.record(() -> {
                log.info("Reserving inventory for order: {}", orderId);

                // Thesis testing - artificial delay for timeout scenarios
                if (artificialDelayEnabled && artificialDelayMs > 0) {
                    log.warn("Artificial delay enabled: sleeping for {} ms (thesis timeout test)", artificialDelayMs);
                    try {
                        Thread.sleep(artificialDelayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("Artificial delay interrupted for order: {}", orderId);
                    }
                }

                // Record received message and latency
                metricsHelper.recordMessageReceived(orderId, SagaMetrics.TYPE_EVENT);
                if (paymentEvent.getCreatedAt() != null) {
                    Duration latency = Duration.between(paymentEvent.getCreatedAt(), Instant.now());
                    metricsHelper.recordMessageLatency("payment", "inventory", latency);
                }

                String reservationId = UUID.randomUUID().toString();
                List<InventoryReservedEvent.ReservedItem> reservedItems = new ArrayList<>();

                try {
                    // Batch query all products at once to avoid N+1 query problem
                    List<String> productIds = items.stream()
                        .map(ItemToReserve::productId)
                        .collect(Collectors.toList());

                    Map<String, Product> products;
                    try {
                        List<Product> productList = productRepository.findAllByProductIdIn(productIds);
                        products = productList.stream()
                            .collect(Collectors.toMap(Product::getProductId, p -> p));
                    } catch (DataAccessException e) {
                        log.error("Database error while finding products for order: {}", orderId, e);
                        throw e;
                    }

                    // Validate all products exist
                    for (String productId : productIds) {
                        if (!products.containsKey(productId)) {
                            throw new ProductNotFoundException(productId);
                        }
                    }

                    // Collect all products and reservations for batch operations
                    List<Product> productsToUpdate = new ArrayList<>();
                    List<InventoryReservation> reservationsToSave = new ArrayList<>();

                    // Process all items with pre-loaded products
                    for (ItemToReserve item : items) {
                        Product product = products.get(item.productId());

                        if (!product.canReserve(item.quantity())) {
                            throw new InsufficientStockException(item.productId(), item.quantity(), product.getQuantityAvailable());
                        }

                        product.reserve(item.quantity());
                        productsToUpdate.add(product);

                        InventoryReservation reservation = InventoryReservation.builder()
                            .reservationId(reservationId + "-" + item.productId())
                            .orderId(orderId)
                            .productId(item.productId())
                            .quantity(item.quantity())
                            .status(InventoryReservation.ReservationStatus.RESERVED)
                            .build();
                        reservationsToSave.add(reservation);

                        reservedItems.add(InventoryReservedEvent.ReservedItem.builder()
                            .productId(item.productId())
                            .quantity(item.quantity())
                            .build());
                    }

                    // Batch save all products and reservations
                    try {
                        productRepository.saveAll(productsToUpdate);
                        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_INVENTORY);

                        reservationRepository.saveAll(reservationsToSave);
                        metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_INVENTORY);
                    } catch (DataAccessException e) {
                        log.error("Database error while batch saving products/reservations for order: {}", orderId, e);
                        throw e;
                    }

                    String correlationId = MDC.get("correlationId");
                    if (correlationId == null || correlationId.isBlank()) {
                        correlationId = paymentEvent.getCorrelationId() != null ?
                            paymentEvent.getCorrelationId() : UUID.randomUUID().toString();
                        MDC.put("correlationId", correlationId);
                    }

                    InventoryReservedEvent event = InventoryReservedEvent.builder()
                        .reservationId(reservationId)
                        .orderId(orderId)
                        .reservedItems(reservedItems)
                        .reservedAt(Instant.now())
                        .correlationId(correlationId)
                        .createdAt(Instant.now())
                        .build();

                    // Publish event after transaction commit
                    final String finalCorrelationId = correlationId;
                    if (TransactionSynchronizationManager.isSynchronizationActive()) {
                        TransactionSynchronizationManager.registerSynchronization(
                            new TransactionSynchronization() {
                                @Override
                                public void afterCommit() {
                                    try {
                                        MDC.put("orderId", orderId);
                                        MDC.put("correlationId", finalCorrelationId);
                                        eventPublisher.publishInventoryReserved(event);
                                        metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                        reservationSuccessCounter.increment();
                                        sagaStepsExecutedCounter.increment();
                                        log.info("Inventory reserved for order: {}", orderId);
                                    } catch (Exception e) {
                                        log.error("Failed to publish InventoryReservedEvent after commit for order: {}", orderId, e);
                                    } finally {
                                        MDC.clear();
                                    }
                                }
                            }
                        );
                    } else {
                        eventPublisher.publishInventoryReserved(event);
                        metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                        reservationSuccessCounter.increment();
                        sagaStepsExecutedCounter.increment();
                        log.info("Inventory reserved for order: {}", orderId);
                    }

                } catch (ProductNotFoundException | InsufficientStockException e) {
                    log.error("Business logic error while reserving inventory for order: {}", orderId, e);
                    handleReservationFailure(orderId, e);
                } catch (DataAccessException e) {
                    log.error("Database error while reserving inventory for order: {}", orderId, e);
                    handleReservationFailure(orderId, e);
                } catch (IllegalArgumentException e) {
                    log.error("Invalid argument while reserving inventory for order: {}", orderId, e);
                    handleReservationFailure(orderId, e);
                } catch (Exception e) {
                    log.error("Unexpected error while reserving inventory for order: {}", orderId, e);
                    handleReservationFailure(orderId, e);
                }
            });
        } finally {
            MDC.clear();
        }
    }

    private void handleReservationFailure(String orderId, Exception e) {
        try {
            String correlationId = MDC.get("correlationId");
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
                MDC.put("correlationId", correlationId);
            }

            InventoryReservationFailedEvent failedEvent = InventoryReservationFailedEvent.builder()
                .orderId(orderId)
                .reason(e.getMessage())
                .failedAt(Instant.now())
                .correlationId(correlationId)
                .createdAt(Instant.now())
                .build();

            // Publish event after transaction commit
            final String finalCorrelationId = correlationId;
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            try {
                                MDC.put("orderId", orderId);
                                MDC.put("correlationId", finalCorrelationId);
                                eventPublisher.publishInventoryReservationFailed(failedEvent);
                                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                reservationFailedCounter.increment();
                                sagaStepsFailedCounter.increment();
                            } catch (Exception ex) {
                                log.error("Failed to publish InventoryReservationFailedEvent after commit for order: {}", orderId, ex);
                            } finally {
                                MDC.clear();
                            }
                        }
                    }
                );
            } else {
                eventPublisher.publishInventoryReservationFailed(failedEvent);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                reservationFailedCounter.increment();
                sagaStepsFailedCounter.increment();
            }
        } catch (Exception ex) {
            log.error("Error while handling reservation failure for order: {}", orderId, ex);
        }
    }

    @Transactional
    @Observed(name = "inventory.release", contextualName = "release-inventory")
    public void releaseInventory(String orderId) {
        // Input validation
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }

        long startTime = System.currentTimeMillis();
        try {
            MDC.put("orderId", orderId);
            log.info("Releasing inventory for order: {}", orderId);

            List<InventoryReservation> reservations;
            try {
                reservations = reservationRepository.findByOrderId(orderId);
            } catch (DataAccessException e) {
                log.error("Database error while finding reservations for order: {}", orderId, e);
                throw e;
            }

            // Filter only RESERVED reservations
            List<InventoryReservation> reservedReservations = reservations.stream()
                .filter(r -> r.getStatus() == InventoryReservation.ReservationStatus.RESERVED)
                .toList();

            if (reservedReservations.isEmpty()) {
                log.info("No reserved inventory to release for order: {}", orderId);
                return;
            }

            // Batch query all products at once
            List<String> productIds = reservedReservations.stream()
                .map(InventoryReservation::getProductId)
                .distinct()
                .collect(Collectors.toList());

            Map<String, Product> products;
            try {
                List<Product> productList = productRepository.findAllByProductIdIn(productIds);
                products = productList.stream()
                    .collect(Collectors.toMap(Product::getProductId, p -> p));
            } catch (DataAccessException e) {
                log.error("Database error while finding products for order: {}", orderId, e);
                throw e;
            }

            // Validate all products exist
            for (String productId : productIds) {
                if (!products.containsKey(productId)) {
                    throw new ProductNotFoundException(productId);
                }
            }

            // Collect all products and reservations for batch operations
            List<Product> productsToUpdate = new ArrayList<>();
            List<InventoryReservation> reservationsToUpdate = new ArrayList<>();

            for (InventoryReservation reservation : reservedReservations) {
                Product product = products.get(reservation.getProductId());
                product.release(reservation.getQuantity());
                productsToUpdate.add(product);

                reservation.setStatus(InventoryReservation.ReservationStatus.RELEASED);
                reservationsToUpdate.add(reservation);
            }

            // Batch save all products and reservations
            try {
                productRepository.saveAll(productsToUpdate);
                metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_INVENTORY);

                reservationRepository.saveAll(reservationsToUpdate);
                metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_INVENTORY);
            } catch (DataAccessException e) {
                log.error("Database error while batch saving products/reservations for order: {}", orderId, e);
                throw e;
            }

            String correlationId = MDC.get("correlationId");
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
                MDC.put("correlationId", correlationId);
            }

            InventoryReleasedEvent event = InventoryReleasedEvent.builder()
                .orderId(orderId)
                .releasedAt(Instant.now())
                .correlationId(correlationId)
                .createdAt(Instant.now())
                .build();

            // Publish event after transaction commit
            final String finalCorrelationId = correlationId;
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            try {
                                MDC.put("orderId", orderId);
                                MDC.put("correlationId", finalCorrelationId);
                                eventPublisher.publishInventoryReleased(event);
                                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                            } catch (Exception e) {
                                log.error("Failed to publish InventoryReleasedEvent after commit for order: {}", orderId, e);
                            } finally {
                                MDC.clear();
                            }
                        }
                    }
                );
            } else {
                eventPublisher.publishInventoryReleased(event);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
            }

            compensationInventoryCounter.increment();
            compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));

            log.info("Inventory released for order: {}", orderId);
        } finally {
            MDC.clear();
        }
    }

    public record ItemToReserve(String productId, int quantity) {
    }
}
