package com.thesis.choreography.inventory.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.events.InventoryReleasedEvent;
import com.thesis.common.events.InventoryReservationFailedEvent;
import com.thesis.common.events.InventoryReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsConfig topicsConfig;

    public void publishInventoryReserved(InventoryReservedEvent event) {
        log.info("Publishing InventoryReservedEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(topicsConfig.getInventoryEvents(), event.getOrderId(), event);
    }

    public void publishInventoryReservationFailed(InventoryReservationFailedEvent event) {
        log.info("Publishing InventoryReservationFailedEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(topicsConfig.getInventoryEvents(), event.getOrderId(), event);
    }

    public void publishInventoryReleased(InventoryReleasedEvent event) {
        log.info("Publishing InventoryReleasedEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(topicsConfig.getInventoryEvents(), event.getOrderId(), event);
    }
}
