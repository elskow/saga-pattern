package com.thesis.choreography.order.kafka;

import com.thesis.common.config.KafkaTopicsProperties;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.kafka.AbstractEventPublisher;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher extends AbstractEventPublisher {

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                               KafkaTopicsProperties topics,
                               MeterRegistry meterRegistry) {
        super(kafkaTemplate, topics, meterRegistry);
    }

    @Override
    protected String getTopic() {
        return topics().orderEvents();
    }

    @Override
    protected String getServiceName() {
        return SagaMetrics.SERVICE_CHOREOGRAPHY;
    }

    public void publishOrderCreated(OrderCreatedEvent event) {
        publishEvent("OrderCreatedEvent", event, event.orderId());
    }
}
