package com.thesis.choreography.shipping.service;

import com.thesis.choreography.shipping.kafka.ShippingEventPublisher;
import com.thesis.choreography.shipping.model.Shipment;
import com.thesis.choreography.shipping.repository.ShipmentRepository;
import com.thesis.common.exception.ShipmentNotFoundException;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.ShippingCancelledEvent;
import com.thesis.common.events.ShippingFailedEvent;
import com.thesis.common.events.ShippingScheduledEvent;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
@Slf4j
public class ShippingService {

    private final ShipmentRepository shipmentRepository;
    private final ShippingEventPublisher eventPublisher;
    private final Counter shippingSuccessCounter;
    private final Counter shippingFailedCounter;
    private final Timer shippingTimer;
    private final Timer stepShippingDurationTimer;
    private final Counter compensationShippingCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final SagaMetricsHelper metricsHelper;

    public ShippingService(ShipmentRepository shipmentRepository,
                           ShippingEventPublisher eventPublisher,
                           MeterRegistry meterRegistry) {
        this.shipmentRepository = shipmentRepository;
        this.eventPublisher = eventPublisher;
        
        this.shippingSuccessCounter = meterRegistry.counter(SagaMetrics.SHIPPING_SUCCESS, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.shippingFailedCounter = meterRegistry.counter(SagaMetrics.SHIPPING_FAILED, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.shippingTimer = meterRegistry.timer(SagaMetrics.SHIPPING_PROCESSING_TIME, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.stepShippingDurationTimer = meterRegistry.timer(SagaMetrics.STEP_SHIPPING_DURATION, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.compensationShippingCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_SHIPPING, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.compensationDurationTimer = meterRegistry.timer(SagaMetrics.COMPENSATION_DURATION, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.sagaStepsExecutedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_EXECUTED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.sagaStepsFailedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_CHOREOGRAPHY);
    }

    @Transactional
    @Observed(name = "shipping.schedule", contextualName = "schedule-shipping")
    public void scheduleShipping(InventoryReservedEvent inventoryEvent, String shippingAddress) {
        // Input validation
        if (inventoryEvent == null) {
            throw new IllegalArgumentException("InventoryReservedEvent cannot be null");
        }
        if (inventoryEvent.getOrderId() == null || inventoryEvent.getOrderId().isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }
        if (shippingAddress == null || shippingAddress.isBlank()) {
            throw new IllegalArgumentException("Shipping address cannot be null or blank");
        }
        
        String orderId = inventoryEvent.getOrderId();
        try {
            MDC.put("orderId", orderId);
            stepShippingDurationTimer.record(() -> {
                log.info("Scheduling shipping for order: {}", orderId);
                
                // Record received message and latency
                metricsHelper.recordMessageReceived(orderId, SagaMetrics.TYPE_EVENT);
                if (inventoryEvent.getCreatedAt() != null) {
                    Duration latency = Duration.between(inventoryEvent.getCreatedAt(), Instant.now());
                    metricsHelper.recordMessageLatency("inventory", "shipping", latency);
                }
                
                String shippingId = UUID.randomUUID().toString();
                String trackingNumber = "TRK-" + UUID.randomUUID().toString().substring(0, 10).toUpperCase();
                
                Shipment shipment = Shipment.builder()
                        .shippingId(shippingId)
                        .orderId(orderId)
                        .trackingNumber(trackingNumber)
                        .shippingAddress(shippingAddress != null ? shippingAddress : "Default Address")
                        .status(Shipment.ShippingStatus.PENDING)
                        .estimatedDelivery(Instant.now().plus(3, ChronoUnit.DAYS))
                        .build();
                
                try {
                    shipmentRepository.save(shipment);
                    metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_SHIPMENT);
                } catch (DataAccessException e) {
                    log.error("Database error while saving shipment for order: {}", orderId, e);
                    throw e;
                }

                try {
                    // Schedule shipping
                    shipment.setStatus(Shipment.ShippingStatus.SCHEDULED);
                    shipmentRepository.save(shipment);
                    metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_SHIPMENT);

                    String correlationId = MDC.get("correlationId");
                    if (correlationId == null || correlationId.isBlank()) {
                        correlationId = inventoryEvent.getCorrelationId() != null ? 
                                inventoryEvent.getCorrelationId() : UUID.randomUUID().toString();
                        MDC.put("correlationId", correlationId);
                    }
                    
                    ShippingScheduledEvent event = ShippingScheduledEvent.builder()
                            .shippingId(shippingId)
                            .orderId(orderId)
                            .trackingNumber(trackingNumber)
                            .address(shipment.getShippingAddress())
                            .estimatedDelivery(shipment.getEstimatedDelivery())
                            .scheduledAt(Instant.now())
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
                                            eventPublisher.publishShippingScheduled(event);
                                            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                            shippingSuccessCounter.increment();
                                            sagaStepsExecutedCounter.increment();
                                            log.info("Shipping scheduled for order: {} with tracking: {}", orderId, trackingNumber);
                                        } catch (Exception e) {
                                            log.error("Failed to publish ShippingScheduledEvent after commit for order: {}", orderId, e);
                                        } finally {
                                            MDC.clear();
                                        }
                                    }
                                }
                        );
                    } else {
                        eventPublisher.publishShippingScheduled(event);
                        metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                        shippingSuccessCounter.increment();
                        sagaStepsExecutedCounter.increment();
                        log.info("Shipping scheduled for order: {} with tracking: {}", orderId, trackingNumber);
                    }
                } catch (IllegalArgumentException e) {
                    log.error("Invalid argument while scheduling shipping for order: {}", orderId, e);
                    handleShippingFailure(shipment, orderId, e);
                } catch (DataAccessException e) {
                    log.error("Database error while scheduling shipping for order: {}", orderId, e);
                    handleShippingFailure(shipment, orderId, e);
                } catch (Exception e) {
                    log.error("Unexpected error while scheduling shipping for order: {}", orderId, e);
                    handleShippingFailure(shipment, orderId, e);
                }
            });
        } finally {
            MDC.clear();
        }
    }
    
    private void handleShippingFailure(Shipment shipment, String orderId, Exception e) {
        try {
            shipment.setStatus(Shipment.ShippingStatus.FAILED);
            shipmentRepository.save(shipment);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_SHIPMENT);

            String correlationId = MDC.get("correlationId");
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
                MDC.put("correlationId", correlationId);
            }
            
            ShippingFailedEvent failedEvent = ShippingFailedEvent.builder()
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
                                    eventPublisher.publishShippingFailed(failedEvent);
                                    metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                    shippingFailedCounter.increment();
                                    sagaStepsFailedCounter.increment();
                                } catch (Exception ex) {
                                    log.error("Failed to publish ShippingFailedEvent after commit for order: {}", orderId, ex);
                                } finally {
                                    MDC.clear();
                                }
                            }
                        }
                );
            } else {
                eventPublisher.publishShippingFailed(failedEvent);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                shippingFailedCounter.increment();
                sagaStepsFailedCounter.increment();
            }
        } catch (Exception ex) {
            log.error("Error while handling shipping failure for order: {}", orderId, ex);
        }
    }

    @Transactional
    @Observed(name = "shipping.cancel", contextualName = "cancel-shipping")
    public void cancelShipping(String orderId) {
        // Input validation
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }
        
        long startTime = System.currentTimeMillis();
        try {
            MDC.put("orderId", orderId);
            log.info("Cancelling shipping for order: {}", orderId);
            
            Shipment shipment;
            try {
                shipment = shipmentRepository.findByOrderId(orderId)
                        .orElseThrow(() -> new ShipmentNotFoundException(orderId));
            } catch (DataAccessException e) {
                log.error("Database error while finding shipment for order: {}", orderId, e);
                throw e;
            }
            
            if (shipment.getStatus() == Shipment.ShippingStatus.SCHEDULED ||
                shipment.getStatus() == Shipment.ShippingStatus.PENDING) {
                shipment.setStatus(Shipment.ShippingStatus.CANCELLED);
                try {
                    shipmentRepository.save(shipment);
                    metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_SHIPMENT);
                } catch (DataAccessException e) {
                    log.error("Database error while saving cancelled shipment for order: {}", orderId, e);
                    throw e;
                }

            String correlationId = MDC.get("correlationId");
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
                MDC.put("correlationId", correlationId);
            }
            
            ShippingCancelledEvent event = ShippingCancelledEvent.builder()
                    .shippingId(shipment.getShippingId())
                    .orderId(orderId)
                    .cancelledAt(Instant.now())
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
                                    eventPublisher.publishShippingCancelled(event);
                                    metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                } catch (Exception e) {
                                    log.error("Failed to publish ShippingCancelledEvent after commit for order: {}", orderId, e);
                                } finally {
                                    MDC.clear();
                                }
                            }
                        }
                );
            } else {
                eventPublisher.publishShippingCancelled(event);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
            }

                compensationShippingCounter.increment();
                compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
                
                log.info("Shipping cancelled for order: {}", orderId);
            }
        } finally {
            MDC.clear();
        }
    }

    public SagaMetricsHelper getMetricsHelper() {
        return metricsHelper;
    }
}
