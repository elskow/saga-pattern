package com.thesis.choreography.inventory.service;

import com.thesis.choreography.inventory.kafka.InventoryEventPublisher;
import com.thesis.choreography.inventory.model.InventoryReservation;
import com.thesis.choreography.inventory.model.Product;
import com.thesis.choreography.inventory.repository.InventoryReservationRepository;
import com.thesis.choreography.inventory.repository.ProductRepository;
import com.thesis.common.exception.InsufficientStockException;
import com.thesis.common.exception.ProductNotFoundException;
import com.thesis.common.events.InventoryReleasedEvent;
import com.thesis.common.events.InventoryReservationFailedEvent;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class InventoryService {

    private final ProductRepository productRepository;
    private final InventoryReservationRepository reservationRepository;
    private final InventoryEventPublisher eventPublisher;
    private final Counter reservationSuccessCounter;
    private final Counter reservationFailedCounter;
    private final Timer reservationTimer;
    private final Timer stepInventoryDurationTimer;
    private final Counter compensationInventoryCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final SagaMetricsHelper metricsHelper;

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
        this.reservationTimer = meterRegistry.timer(SagaMetrics.INVENTORY_RESERVATION_TIME, 
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
        stepInventoryDurationTimer.record(() -> {
            log.info("Reserving inventory for order: {}", paymentEvent.getOrderId());
            
            // Record received message and latency
            metricsHelper.recordMessageReceived(paymentEvent.getOrderId(), SagaMetrics.TYPE_EVENT);
            if (paymentEvent.getCreatedAt() != null) {
                Duration latency = Duration.between(paymentEvent.getCreatedAt(), Instant.now());
                metricsHelper.recordMessageLatency("payment", "inventory", latency);
            }
            
            String reservationId = UUID.randomUUID().toString();
            List<InventoryReservedEvent.ReservedItem> reservedItems = new ArrayList<>();
            
            try {
                for (ItemToReserve item : items) {
                    Product product = productRepository.findByProductId(item.productId())
                            .orElseThrow(() -> new ProductNotFoundException(item.productId()));
                    
                    if (!product.canReserve(item.quantity())) {
                        throw new InsufficientStockException(item.productId(), item.quantity(), product.getQuantityAvailable());
                    }
                    
                    product.reserve(item.quantity());
                    productRepository.save(product);
                    metricsHelper.recordDbUpdate(paymentEvent.getOrderId(), SagaMetrics.ENTITY_INVENTORY);
                    
                    InventoryReservation reservation = InventoryReservation.builder()
                            .reservationId(reservationId + "-" + item.productId())
                            .orderId(paymentEvent.getOrderId())
                            .productId(item.productId())
                            .quantity(item.quantity())
                            .status(InventoryReservation.ReservationStatus.RESERVED)
                            .build();
                    reservationRepository.save(reservation);
                    metricsHelper.recordDbInsert(paymentEvent.getOrderId(), SagaMetrics.ENTITY_INVENTORY);
                    
                    reservedItems.add(InventoryReservedEvent.ReservedItem.builder()
                            .productId(item.productId())
                            .quantity(item.quantity())
                            .build());
                }
                
                InventoryReservedEvent event = InventoryReservedEvent.builder()
                        .reservationId(reservationId)
                        .orderId(paymentEvent.getOrderId())
                        .reservedItems(reservedItems)
                        .reservedAt(Instant.now())
                        .createdAt(Instant.now())
                        .build();
                
                eventPublisher.publishInventoryReserved(event);
                metricsHelper.recordMessageSent(paymentEvent.getOrderId(), SagaMetrics.TYPE_EVENT);
                reservationSuccessCounter.increment();
                sagaStepsExecutedCounter.increment();
                log.info("Inventory reserved for order: {}", paymentEvent.getOrderId());
                
            } catch (Exception e) {
                log.error("Failed to reserve inventory for order: {}", paymentEvent.getOrderId(), e);
                
                InventoryReservationFailedEvent failedEvent = InventoryReservationFailedEvent.builder()
                        .orderId(paymentEvent.getOrderId())
                        .reason(e.getMessage())
                        .failedAt(Instant.now())
                        .createdAt(Instant.now())
                        .build();
                
                eventPublisher.publishInventoryReservationFailed(failedEvent);
                metricsHelper.recordMessageSent(paymentEvent.getOrderId(), SagaMetrics.TYPE_EVENT);
                reservationFailedCounter.increment();
                sagaStepsFailedCounter.increment();
            }
        });
    }

    @Transactional
    @Observed(name = "inventory.release", contextualName = "release-inventory")
    public void releaseInventory(String orderId) {
        long startTime = System.currentTimeMillis();
        log.info("Releasing inventory for order: {}", orderId);
        
        List<InventoryReservation> reservations = reservationRepository.findByOrderId(orderId);
        
        for (InventoryReservation reservation : reservations) {
            if (reservation.getStatus() == InventoryReservation.ReservationStatus.RESERVED) {
                Product product = productRepository.findByProductId(reservation.getProductId())
                        .orElseThrow(() -> new ProductNotFoundException(reservation.getProductId()));
                product.release(reservation.getQuantity());
                productRepository.save(product);
                metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_INVENTORY);
                
                reservation.setStatus(InventoryReservation.ReservationStatus.RELEASED);
                reservationRepository.save(reservation);
                metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_INVENTORY);
            }
        }
        
        InventoryReleasedEvent event = InventoryReleasedEvent.builder()
                .orderId(orderId)
                .releasedAt(Instant.now())
                .createdAt(Instant.now())
                .build();
        
        eventPublisher.publishInventoryReleased(event);
        metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
        
        compensationInventoryCounter.increment();
        compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
        
        log.info("Inventory released for order: {}", orderId);
    }

    public SagaMetricsHelper getMetricsHelper() {
        return metricsHelper;
    }

    public record ItemToReserve(String productId, int quantity) {}
}
