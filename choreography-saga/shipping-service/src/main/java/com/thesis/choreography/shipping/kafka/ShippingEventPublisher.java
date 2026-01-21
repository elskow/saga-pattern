package com.thesis.choreography.shipping.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.events.ShippingCancelledEvent;
import com.thesis.common.events.ShippingFailedEvent;
import com.thesis.common.events.ShippingScheduledEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ShippingEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsConfig topicsConfig;

    public void publishShippingScheduled(ShippingScheduledEvent event) {
        log.info("Publishing ShippingScheduledEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(topicsConfig.getShippingEvents(), event.getOrderId(), event);
    }

    public void publishShippingFailed(ShippingFailedEvent event) {
        log.info("Publishing ShippingFailedEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(topicsConfig.getShippingEvents(), event.getOrderId(), event);
    }

    public void publishShippingCancelled(ShippingCancelledEvent event) {
        log.info("Publishing ShippingCancelledEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(topicsConfig.getShippingEvents(), event.getOrderId(), event);
    }
}
