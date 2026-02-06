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

import com.fasterxml.jackson.core.type.TypeReference;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class InventoryService {

    private final ReservationRepository reservationRepository;
    private final ProductRepository productRepository;
    private final SagaMetricsHelper metricsHelper;
    private final ObjectMapper objectMapper;

    private static final TypeReference<List<OrderCreatedEvent.OrderItemEvent>> ITEMS_TYPE_REF =
            new TypeReference<>() {};

    @Value("${app.artificial-delay.enabled:false}")
    private boolean artificialDelayEnabled;

    @Value("${app.artificial-delay.duration-ms:0}")
    private long artificialDelayMs;

    public InventoryService(ReservationRepository reservationRepository,
                            ProductRepository productRepository,
                            MeterRegistry meterRegistry,
                            ObjectMapper objectMapper) {
        this.reservationRepository = reservationRepository;
        this.productRepository = productRepository;
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ReservationResult reserveInventory(ReserveInventoryCommand command) {
        log.debug("Reserving inventory {} for order {}",
            command.reservationId(), command.orderId());

        if (artificialDelayEnabled && artificialDelayMs > 0) {
            log.warn("Artificial delay enabled: sleeping for {} ms (thesis timeout test)", artificialDelayMs);
            try {
                Thread.sleep(artificialDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Artificial delay interrupted for order: {}", command.orderId());
            }
        }

        var existingResultOpt = reservationRepository.findById(command.reservationId())
            .flatMap(existing -> {
                log.debug("Reservation {} exists with status {}", command.reservationId(), existing.getStatus());
                return switch (existing.getStatus()) {
                    case RESERVED -> Optional.of(ReservationResult.alreadyExists(existing));
                    case FAILED -> Optional.of(ReservationResult.failure("Reservation previously failed: " + existing.getFailureReason()));
                    default -> { log.warn("Attempting re-reservation"); yield Optional.empty(); }
                };
            });
        if (existingResultOpt.isPresent()) return existingResultOpt.get();

        List<OrderCreatedEvent.OrderItemEvent> items = command.items();

        List<String> productIds = items.stream()
            .map(OrderCreatedEvent.OrderItemEvent::productId)
            .distinct()
            .toList();

        List<ProductEntity> products = productRepository.findAllByIdInForUpdate(productIds);
        Map<String, ProductEntity> productMap = products.stream()
            .collect(Collectors.toMap(ProductEntity::getProductId, Function.identity()));

        for (OrderCreatedEvent.OrderItemEvent item : items) {
            ProductEntity product = productMap.get(item.productId());
            if (product == null) {
                String error = "Product not found: %s".formatted(item.productId());
                saveFailedReservation(command, error);
                return ReservationResult.failure(error);
            }
            int available = product.getQuantity() -
                Objects.requireNonNullElse(product.getReservedQuantity(), 0);
            if (available < item.quantity()) {
                String error = "Insufficient stock for product: %s".formatted(item.productId());
                saveFailedReservation(command, error);
                return ReservationResult.failure(error);
            }
        }

        items.forEach(item -> {
            ProductEntity product = productMap.get(item.productId());
            product.setReservedQuantity(Objects.requireNonNullElse(product.getReservedQuantity(), 0) + item.quantity());
            metricsHelper.recordDbUpdate(command.orderId(), SagaMetrics.ENTITY_INVENTORY);
        });
        productRepository.saveAll(products);

        String itemsJson = serializeItems(items, command.orderId());
        ReservationEntity reservation = ReservationEntity.builder()
            .reservationId(command.reservationId())
            .orderId(command.orderId())
            .itemsJson(itemsJson)
            .status(ReservationEntity.ReservationStatus.RESERVED)
            .reservedAt(Instant.now())
            .build();
        reservationRepository.save(reservation);
        metricsHelper.recordDbInsert(command.orderId(), SagaMetrics.ENTITY_INVENTORY);

        log.debug("Inventory {} reserved successfully", command.reservationId());
        return ReservationResult.success(reservation);
    }

    @Transactional
    public boolean releaseInventory(ReleaseInventoryCommand command) {
        log.debug("Releasing inventory {} for order {}", command.reservationId(), command.orderId());

        return reservationRepository.findById(command.reservationId())
            .map(reservation -> {
                if (reservation.getStatus() == ReservationEntity.ReservationStatus.RELEASED) {
                    log.debug("Reservation {} already released", command.reservationId());
                    return true;
                }

                List<OrderCreatedEvent.OrderItemEvent> items = deserializeItems(reservation.getItemsJson());
                if (items != null && !items.isEmpty()) {
                    List<String> productIds = items.stream()
                            .map(OrderCreatedEvent.OrderItemEvent::productId)
                            .distinct()
                            .toList();

                    List<ProductEntity> products = productRepository.findAllById(productIds);
                    Map<String, ProductEntity> productMap = products.stream()
                            .collect(Collectors.toMap(ProductEntity::getProductId, Function.identity()));

                    items.forEach(item -> Optional.ofNullable(productMap.get(item.productId()))
                        .ifPresent(product -> product.setReservedQuantity(
                            Math.max(0, Objects.requireNonNullElse(product.getReservedQuantity(), 0) - item.quantity()))));

                    productRepository.saveAll(products);
                    metricsHelper.recordDbUpdate(command.orderId(), SagaMetrics.ENTITY_INVENTORY);
                }

                reservation.setStatus(ReservationEntity.ReservationStatus.RELEASED);
                reservation.setReleasedAt(Instant.now());
                reservation.setReleaseReason("Order cancelled - saga compensation");
                reservationRepository.save(reservation);
                metricsHelper.recordDbUpdate(command.orderId(), SagaMetrics.ENTITY_INVENTORY);

                log.debug("Inventory {} released successfully", command.reservationId());
                return true;
            })
            .orElseGet(() -> {
                log.warn("Reservation not found: {}", command.reservationId());
                return false;
            });
    }

    private void saveFailedReservation(ReserveInventoryCommand command, String reason) {
        ReservationEntity reservation = ReservationEntity.builder()
            .reservationId(command.reservationId())
            .orderId(command.orderId())
            .itemsJson(serializeItems(command.items(), command.orderId()))
            .status(ReservationEntity.ReservationStatus.FAILED)
            .failureReason(reason)
            .build();
        reservationRepository.save(reservation);
        metricsHelper.recordDbInsert(command.orderId(), SagaMetrics.ENTITY_INVENTORY);
    }

    private String serializeItems(List<OrderCreatedEvent.OrderItemEvent> items, String orderId) {
        try {
            return objectMapper.writeValueAsString(items);
        } catch (Exception e) {
            log.error("Failed to serialize items for order {}: {}", orderId, e.getMessage());
            throw new IllegalStateException("Failed to serialize items for order " + orderId, e);
        }
    }

    private List<OrderCreatedEvent.OrderItemEvent> deserializeItems(String itemsJson) {
        try {
            return objectMapper.readValue(itemsJson, ITEMS_TYPE_REF);
        } catch (Exception e) {
            log.error("Failed to deserialize items: {}", e.getMessage());
            return List.of();
        }
    }

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
