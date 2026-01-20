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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    public InventoryService(ProductRepository productRepository,
                            InventoryReservationRepository reservationRepository,
                            InventoryEventPublisher eventPublisher,
                            MeterRegistry meterRegistry) {
        this.productRepository = productRepository;
        this.reservationRepository = reservationRepository;
        this.eventPublisher = eventPublisher;
        
        this.reservationSuccessCounter = meterRegistry.counter("inventory.reservations.success", "service", "choreography");
        this.reservationFailedCounter = meterRegistry.counter("inventory.reservations.failed", "service", "choreography");
        this.reservationTimer = meterRegistry.timer("inventory.reservation.time", "service", "choreography");
    }

    @Transactional
    public void reserveInventory(PaymentCompletedEvent paymentEvent, List<ItemToReserve> items) {
        reservationTimer.record(() -> {
            log.info("Reserving inventory for order: {}", paymentEvent.getOrderId());
            
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
                    
                    InventoryReservation reservation = InventoryReservation.builder()
                            .reservationId(reservationId + "-" + item.productId())
                            .orderId(paymentEvent.getOrderId())
                            .productId(item.productId())
                            .quantity(item.quantity())
                            .status(InventoryReservation.ReservationStatus.RESERVED)
                            .build();
                    reservationRepository.save(reservation);
                    
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
                        .build();
                
                eventPublisher.publishInventoryReserved(event);
                reservationSuccessCounter.increment();
                log.info("Inventory reserved for order: {}", paymentEvent.getOrderId());
                
            } catch (Exception e) {
                log.error("Failed to reserve inventory for order: {}", paymentEvent.getOrderId(), e);
                
                InventoryReservationFailedEvent failedEvent = InventoryReservationFailedEvent.builder()
                        .orderId(paymentEvent.getOrderId())
                        .reason(e.getMessage())
                        .failedAt(Instant.now())
                        .build();
                
                eventPublisher.publishInventoryReservationFailed(failedEvent);
                reservationFailedCounter.increment();
            }
        });
    }

    @Transactional
    public void releaseInventory(String orderId) {
        log.info("Releasing inventory for order: {}", orderId);
        
        List<InventoryReservation> reservations = reservationRepository.findByOrderId(orderId);
        
        for (InventoryReservation reservation : reservations) {
            if (reservation.getStatus() == InventoryReservation.ReservationStatus.RESERVED) {
                Product product = productRepository.findByProductId(reservation.getProductId())
                        .orElseThrow(() -> new ProductNotFoundException(reservation.getProductId()));
                product.release(reservation.getQuantity());
                productRepository.save(product);
                
                reservation.setStatus(InventoryReservation.ReservationStatus.RELEASED);
                reservationRepository.save(reservation);
            }
        }
        
        InventoryReleasedEvent event = InventoryReleasedEvent.builder()
                .orderId(orderId)
                .releasedAt(Instant.now())
                .build();
        
        eventPublisher.publishInventoryReleased(event);
        log.info("Inventory released for order: {}", orderId);
    }

    public record ItemToReserve(String productId, int quantity) {}
}
