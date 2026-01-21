package com.thesis.choreography.order.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsConfig topicsConfig;

    public void publishOrderCreated(OrderCreatedEvent event) {
        log.info("Publishing OrderCreatedEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(topicsConfig.getOrderEvents(), event.getOrderId(), event);
    }
}
