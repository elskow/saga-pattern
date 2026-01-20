package com.thesis.choreography.shipping.service;

import com.thesis.choreography.shipping.kafka.ShippingEventPublisher;
import com.thesis.choreography.shipping.model.Shipment;
import com.thesis.choreography.shipping.repository.ShipmentRepository;
import com.thesis.common.exception.ShipmentNotFoundException;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.ShippingCancelledEvent;
import com.thesis.common.events.ShippingFailedEvent;
import com.thesis.common.events.ShippingScheduledEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    public ShippingService(ShipmentRepository shipmentRepository,
                           ShippingEventPublisher eventPublisher,
                           MeterRegistry meterRegistry) {
        this.shipmentRepository = shipmentRepository;
        this.eventPublisher = eventPublisher;
        
        this.shippingSuccessCounter = meterRegistry.counter("shipping.success", "service", "choreography");
        this.shippingFailedCounter = meterRegistry.counter("shipping.failed", "service", "choreography");
        this.shippingTimer = meterRegistry.timer("shipping.processing.time", "service", "choreography");
    }

    @Transactional
    public void scheduleShipping(InventoryReservedEvent inventoryEvent, String shippingAddress) {
        shippingTimer.record(() -> {
            log.info("Scheduling shipping for order: {}", inventoryEvent.getOrderId());
            
            String shippingId = UUID.randomUUID().toString();
            String trackingNumber = "TRK-" + UUID.randomUUID().toString().substring(0, 10).toUpperCase();
            
            Shipment shipment = Shipment.builder()
                    .shippingId(shippingId)
                    .orderId(inventoryEvent.getOrderId())
                    .trackingNumber(trackingNumber)
                    .shippingAddress(shippingAddress != null ? shippingAddress : "Default Address")
                    .status(Shipment.ShippingStatus.PENDING)
                    .estimatedDelivery(Instant.now().plus(3, ChronoUnit.DAYS))
                    .build();
            
            shipmentRepository.save(shipment);

            try {
                // Schedule shipping
                shipment.setStatus(Shipment.ShippingStatus.SCHEDULED);
                shipmentRepository.save(shipment);

                ShippingScheduledEvent event = ShippingScheduledEvent.builder()
                        .shippingId(shippingId)
                        .orderId(inventoryEvent.getOrderId())
                        .trackingNumber(trackingNumber)
                        .address(shipment.getShippingAddress())
                        .estimatedDelivery(shipment.getEstimatedDelivery())
                        .scheduledAt(Instant.now())
                        .build();

                eventPublisher.publishShippingScheduled(event);
                shippingSuccessCounter.increment();
                log.info("Shipping scheduled for order: {} with tracking: {}", 
                        inventoryEvent.getOrderId(), trackingNumber);
            } catch (Exception e) {
                shipment.setStatus(Shipment.ShippingStatus.FAILED);
                shipmentRepository.save(shipment);

                ShippingFailedEvent failedEvent = ShippingFailedEvent.builder()
                        .orderId(inventoryEvent.getOrderId())
                        .reason(e.getMessage())
                        .failedAt(Instant.now())
                        .build();

                eventPublisher.publishShippingFailed(failedEvent);
                shippingFailedCounter.increment();
                log.error("Shipping failed for order: {}", inventoryEvent.getOrderId(), e);
            }
        });
    }

    @Transactional
    public void cancelShipping(String orderId) {
        log.info("Cancelling shipping for order: {}", orderId);
        
        Shipment shipment = shipmentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new ShipmentNotFoundException(orderId));
        
        if (shipment.getStatus() == Shipment.ShippingStatus.SCHEDULED ||
            shipment.getStatus() == Shipment.ShippingStatus.PENDING) {
            shipment.setStatus(Shipment.ShippingStatus.CANCELLED);
            shipmentRepository.save(shipment);

            ShippingCancelledEvent event = ShippingCancelledEvent.builder()
                    .shippingId(shipment.getShippingId())
                    .orderId(orderId)
                    .cancelledAt(Instant.now())
                    .build();

            eventPublisher.publishShippingCancelled(event);
            log.info("Shipping cancelled for order: {}", orderId);
        }
    }
}
