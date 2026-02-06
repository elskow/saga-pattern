package com.thesis.choreography.shipping.service;

import com.thesis.choreography.shipping.kafka.ShippingEventPublisher;
import com.thesis.choreography.shipping.model.Shipment;
import com.thesis.choreography.shipping.repository.ShipmentRepository;
import com.thesis.common.config.ShippingProperties;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.ShippingCancelledEvent;
import com.thesis.common.events.ShippingFailedEvent;
import com.thesis.common.events.ShippingScheduledEvent;
import com.thesis.common.exception.ShipmentNotFoundException;
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
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class ShippingService {

    private final ShipmentRepository shipmentRepository;
    private final ShippingEventPublisher eventPublisher;
    private final ShippingProperties shippingProperties;
    private final Counter shippingSuccessCounter;
    private final Counter shippingFailedCounter;
    private final Timer shippingTimer;
    private final Timer stepShippingDurationTimer;
    private final Counter compensationShippingCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    @Getter
    private final SagaMetricsHelper metricsHelper;

    public ShippingService(ShipmentRepository shipmentRepository,
                           ShippingEventPublisher eventPublisher,
                           ShippingProperties shippingProperties,
                           MeterRegistry meterRegistry) {
        this.shippingProperties = shippingProperties;
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
        validateShippingRequest(inventoryEvent, shippingAddress);
        String orderId = inventoryEvent.orderId();
        try {
            MDC.put("orderId", orderId);
            stepShippingDurationTimer.record(() -> {
                log.debug("Scheduling shipping for order: {}", orderId);

                metricsHelper.recordMessageReceived(orderId, SagaMetrics.TYPE_EVENT);
                if (inventoryEvent.createdAt() != null) {
                    Duration latency = Duration.between(inventoryEvent.createdAt(), Instant.now());
                    metricsHelper.recordMessageLatency("inventory", "shipping", latency);
                }

                String shippingId = UUID.randomUUID().toString();
                String trackingNumber = generateTrackingNumber();

                Shipment shipment = Shipment.builder()
                    .shippingId(shippingId)
                    .orderId(orderId)
                    .trackingNumber(trackingNumber)
                    .shippingAddress(shippingAddress)
                    .status(Shipment.ShippingStatus.PENDING)
                    .estimatedDelivery(Instant.now().plus(shippingProperties.estimatedDeliveryDays(), ChronoUnit.DAYS))
                    .build();

                try {
                    shipmentRepository.save(shipment);
                    metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_SHIPMENT);
                } catch (DataAccessException e) {
                    log.error("Database error while saving shipment for order: {}", orderId, e);
                    throw e;
                }

                try {
                    shipment.setStatus(Shipment.ShippingStatus.SCHEDULED);
                    shipmentRepository.save(shipment);
                    metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_SHIPMENT);

                    String correlationId = CorrelationIdResolver.resolve(inventoryEvent.correlationId());
                    MDC.put("correlationId", correlationId);

                    ShippingScheduledEvent event = ShippingScheduledEvent.of(
                        shippingId,
                        orderId,
                        trackingNumber,
                        shipment.getShippingAddress(),
                        shipment.getEstimatedDelivery(),
                        Instant.now(),
                        correlationId
                    );

                    TransactionHelper.executeAfterCommit(() -> {
                        eventPublisher.publishShippingScheduled(event);
                        metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                        shippingSuccessCounter.increment();
                        sagaStepsExecutedCounter.increment();
                        log.debug("Shipping scheduled for order: {} with tracking: {}", orderId, trackingNumber);
                    }, orderId, correlationId);
                } catch (Exception e) {
                    log.error("Error scheduling shipping for order {}: {}", orderId, e.getMessage(), e);
                    handleShippingFailure(shipment, orderId, e);
                }
            });
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    private void handleShippingFailure(Shipment shipment, String orderId, Exception e) {
        try {
            shipment.setStatus(Shipment.ShippingStatus.FAILED);
            shipmentRepository.save(shipment);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_SHIPMENT);

            String correlationId = CorrelationIdResolver.resolve(null);
            MDC.put("correlationId", correlationId);

            ShippingFailedEvent failedEvent = ShippingFailedEvent.of(
                orderId,
                e.getMessage(),
                Instant.now(),
                correlationId
            );

            TransactionHelper.executeAfterCommit(() -> {
                eventPublisher.publishShippingFailed(failedEvent);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                shippingFailedCounter.increment();
                sagaStepsFailedCounter.increment();
            }, orderId, correlationId);
        } catch (Exception ex) {
            log.error("Error while handling shipping failure for order: {}", orderId, ex);
        }
    }

    @Transactional
    @Observed(name = "shipping.cancel", contextualName = "cancel-shipping")
    public void cancelShipping(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }

        long startTime = System.currentTimeMillis();
        try {
            MDC.put("orderId", orderId);
            log.debug("Cancelling shipping for order: {}", orderId);

            Shipment shipment;
            try {
                shipment = shipmentRepository.findByOrderId(orderId)
                    .orElseThrow(() -> new ShipmentNotFoundException(orderId));
            } catch (DataAccessException e) {
                log.error("Database error while finding shipment for order: {}", orderId, e);
                throw e;
            }

            if (shipment.isCancellable()) {
                shipment.setStatus(Shipment.ShippingStatus.CANCELLED);
                try {
                    shipmentRepository.save(shipment);
                    metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_SHIPMENT);
                } catch (DataAccessException e) {
                    log.error("Database error while saving cancelled shipment for order: {}", orderId, e);
                    throw e;
                }

                String correlationId = CorrelationIdResolver.resolve(null);
                MDC.put("correlationId", correlationId);

                ShippingCancelledEvent event = ShippingCancelledEvent.of(
                    shipment.getShippingId(),
                    orderId,
                    Instant.now(),
                    correlationId
                );

                TransactionHelper.executeAfterCommit(() -> {
                    eventPublisher.publishShippingCancelled(event);
                    metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                }, orderId, correlationId);

                compensationShippingCounter.increment();
                compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));

                log.debug("Shipping cancelled for order: {}", orderId);
            } else {
                log.debug("Shipping cancellation skipped for order: {} - current status: {}", orderId, shipment.getStatus());
            }
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    private void validateShippingRequest(InventoryReservedEvent event, String shippingAddress) {
        Validators.requireNonNull(event, "InventoryReservedEvent");
        Validators.requireNonBlank(event.orderId(), "Order ID");
        Validators.requireNonBlank(shippingAddress, "Shipping address");
    }

    private String generateTrackingNumber() {
        return "%s%s".formatted(
            shippingProperties.trackingNumberPrefix(),
            UUID.randomUUID().toString().substring(0, 10).toUpperCase()
        );
    }
}
