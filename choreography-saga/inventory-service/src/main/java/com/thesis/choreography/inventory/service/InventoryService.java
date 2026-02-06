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
import com.thesis.common.util.CorrelationIdResolver;
import com.thesis.common.util.TransactionHelper;
import com.thesis.common.util.Validators;
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
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
        validateReservationRequest(paymentEvent, items);
        String orderId = paymentEvent.orderId();
        try {
            MDC.put("orderId", orderId);
            stepInventoryDurationTimer.record(() -> executeReservation(paymentEvent, items, orderId));
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    private void executeReservation(PaymentCompletedEvent paymentEvent, List<ItemToReserve> items, String orderId) {
        log.debug("Reserving inventory for order: {}", orderId);

        applyArtificialDelayIfEnabled(orderId);
        recordMessageMetrics(paymentEvent, orderId);

        String reservationId = UUID.randomUUID().toString();

        try {
            List<InventoryReservedEvent.ReservedItem> reservedItems = processReservations(items, reservationId, orderId);
            publishReservationSuccess(paymentEvent, orderId, reservationId, reservedItems);
        } catch (Exception e) {
            log.error("Error reserving inventory for order {}: {}", orderId, e.getMessage(), e);
            handleReservationFailure(orderId, e);
        }
    }

    private void applyArtificialDelayIfEnabled(String orderId) {
        if (artificialDelayEnabled && artificialDelayMs > 0) {
            log.warn("Artificial delay enabled: sleeping for {} ms (thesis timeout test)", artificialDelayMs);
            try {
                Thread.sleep(artificialDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Artificial delay interrupted for order: {}", orderId);
            }
        }
    }

    private void recordMessageMetrics(PaymentCompletedEvent paymentEvent, String orderId) {
        metricsHelper.recordMessageReceived(orderId, SagaMetrics.TYPE_EVENT);
        if (paymentEvent.createdAt() != null) {
            Duration latency = Duration.between(paymentEvent.createdAt(), Instant.now());
            metricsHelper.recordMessageLatency("payment", "inventory", latency);
        }
    }

    private List<InventoryReservedEvent.ReservedItem> processReservations(
            List<ItemToReserve> items, String reservationId, String orderId) {
        
        Map<String, Product> products = fetchAndValidateProducts(items, orderId);
        
        List<Product> productsToUpdate = new ArrayList<>();
        List<InventoryReservation> reservationsToSave = new ArrayList<>();
        List<InventoryReservedEvent.ReservedItem> reservedItems = new ArrayList<>();

        for (ItemToReserve item : items) {
            Product product = products.get(item.productId());

            if (!product.canReserve(item.quantity())) {
                throw new InsufficientStockException(item.productId(), item.quantity(), product.getQuantityAvailable());
            }

            product.reserve(item.quantity());
            productsToUpdate.add(product);

            InventoryReservation reservation = InventoryReservation.builder()
                .reservationId("%s-%s".formatted(reservationId, item.productId()))
                .orderId(orderId)
                .productId(item.productId())
                .quantity(item.quantity())
                .status(InventoryReservation.ReservationStatus.RESERVED)
                .build();
            reservationsToSave.add(reservation);

            reservedItems.add(InventoryReservedEvent.ReservedItem.of(item.productId(), item.quantity()));
        }

        saveReservationData(productsToUpdate, reservationsToSave, orderId);
        return reservedItems;
    }

    private Map<String, Product> fetchAndValidateProducts(List<ItemToReserve> items, String orderId) {
        List<String> productIds = items.stream()
            .map(ItemToReserve::productId)
            .toList();

        Map<String, Product> products;
        try {
            List<Product> productList = productRepository.findAllByProductIdIn(productIds);
            products = productList.stream()
                .collect(Collectors.toMap(Product::getProductId, Function.identity()));
        } catch (DataAccessException e) {
            log.error("Database error while finding products for order: {}", orderId, e);
            throw e;
        }

        productIds.stream()
            .filter(id -> !products.containsKey(id))
            .findFirst()
            .ifPresent(id -> { throw new ProductNotFoundException(id); });

        return products;
    }

    private void saveReservationData(List<Product> products, List<InventoryReservation> reservations, String orderId) {
        try {
            productRepository.saveAll(products);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_INVENTORY);

            reservationRepository.saveAll(reservations);
            metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_INVENTORY);
        } catch (DataAccessException e) {
            log.error("Database error while batch saving products/reservations for order: {}", orderId, e);
            throw e;
        }
    }

    private void publishReservationSuccess(PaymentCompletedEvent paymentEvent, String orderId,
            String reservationId, List<InventoryReservedEvent.ReservedItem> reservedItems) {
        
        String correlationId = CorrelationIdResolver.resolve(paymentEvent.correlationId());
        MDC.put("correlationId", correlationId);

        InventoryReservedEvent event = InventoryReservedEvent.of(
            reservationId,
            orderId,
            reservedItems,
            Instant.now(),
            correlationId
        );

        TransactionHelper.executeAfterCommit(() -> {
            eventPublisher.publishInventoryReserved(event);
            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
            reservationSuccessCounter.increment();
            sagaStepsExecutedCounter.increment();
            log.debug("Inventory reserved for order: {}", orderId);
        }, orderId, correlationId);
    }

    private void handleReservationFailure(String orderId, Exception e) {
        try {
            String correlationId = CorrelationIdResolver.resolve(null);
            MDC.put("correlationId", correlationId);

            InventoryReservationFailedEvent failedEvent = InventoryReservationFailedEvent.of(
                orderId,
                null,
                e.getMessage(),
                Instant.now(),
                correlationId
            );

            TransactionHelper.executeAfterCommit(() -> {
                eventPublisher.publishInventoryReservationFailed(failedEvent);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                reservationFailedCounter.increment();
                sagaStepsFailedCounter.increment();
            }, orderId, correlationId);
        } catch (Exception ex) {
            log.error("Error while handling reservation failure for order: {}", orderId, ex);
        }
    }

    @Transactional
    @Observed(name = "inventory.release", contextualName = "release-inventory")
    public void releaseInventory(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }

        long startTime = System.currentTimeMillis();
        try {
            MDC.put("orderId", orderId);
            log.debug("Releasing inventory for order: {}", orderId);

            List<InventoryReservation> reservedReservations = findReservedReservations(orderId);
            if (reservedReservations.isEmpty()) {
                log.debug("No reserved inventory to release for order: {}", orderId);
                return;
            }

            releaseReservedInventory(reservedReservations, orderId);
            publishReleaseEvent(orderId);
            recordCompensationMetrics(startTime);

            log.debug("Inventory released for order: {}", orderId);
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    private List<InventoryReservation> findReservedReservations(String orderId) {
        List<InventoryReservation> reservations;
        try {
            reservations = reservationRepository.findByOrderId(orderId);
        } catch (DataAccessException e) {
            log.error("Database error while finding reservations for order: {}", orderId, e);
            throw e;
        }

        return reservations.stream()
            .filter(r -> r.getStatus() == InventoryReservation.ReservationStatus.RESERVED)
            .toList();
    }

    private void releaseReservedInventory(List<InventoryReservation> reservedReservations, String orderId) {
        List<String> productIds = reservedReservations.stream()
            .map(InventoryReservation::getProductId)
            .distinct()
            .toList();

        Map<String, Product> products = fetchProductsForRelease(productIds, orderId);

        List<Product> productsToUpdate = new ArrayList<>();
        List<InventoryReservation> reservationsToUpdate = new ArrayList<>();

        for (InventoryReservation reservation : reservedReservations) {
            Product product = products.get(reservation.getProductId());
            product.release(reservation.getQuantity());
            productsToUpdate.add(product);

            reservation.setStatus(InventoryReservation.ReservationStatus.RELEASED);
            reservationsToUpdate.add(reservation);
        }

        saveReleaseData(productsToUpdate, reservationsToUpdate, orderId);
    }

    private Map<String, Product> fetchProductsForRelease(List<String> productIds, String orderId) {
        Map<String, Product> products;
        try {
            List<Product> productList = productRepository.findAllByProductIdIn(productIds);
            products = productList.stream()
                .collect(Collectors.toMap(Product::getProductId, Function.identity()));
        } catch (DataAccessException e) {
            log.error("Database error while finding products for order: {}", orderId, e);
            throw e;
        }

        productIds.stream()
            .filter(id -> !products.containsKey(id))
            .findFirst()
            .ifPresent(id -> { throw new ProductNotFoundException(id); });

        return products;
    }

    private void saveReleaseData(List<Product> products, List<InventoryReservation> reservations, String orderId) {
        try {
            productRepository.saveAll(products);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_INVENTORY);

            reservationRepository.saveAll(reservations);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_INVENTORY);
        } catch (DataAccessException e) {
            log.error("Database error while batch saving products/reservations for order: {}", orderId, e);
            throw e;
        }
    }

    private void publishReleaseEvent(String orderId) {
        String correlationId = CorrelationIdResolver.resolve(null);
        MDC.put("correlationId", correlationId);

        InventoryReleasedEvent event = InventoryReleasedEvent.of(
            null,
            orderId,
            Instant.now(),
            correlationId
        );

        TransactionHelper.executeAfterCommit(() -> {
            eventPublisher.publishInventoryReleased(event);
            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
        }, orderId, correlationId);
    }

    private void recordCompensationMetrics(long startTime) {
        compensationInventoryCounter.increment();
        compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
    }

    public record ItemToReserve(String productId, int quantity) {
        public ItemToReserve {
            if (productId == null || productId.isBlank()) {
                throw new IllegalArgumentException("Product ID cannot be null or blank");
            }
            if (quantity <= 0) {
                throw new IllegalArgumentException("Quantity must be greater than zero");
            }
        }
    }

    private void validateReservationRequest(PaymentCompletedEvent event, List<ItemToReserve> items) {
        Validators.requireNonNull(event, "PaymentCompletedEvent");
        Validators.requireNonBlank(event.orderId(), "Order ID");
        Validators.requireNotEmpty(items, "Items list");
    }
}
