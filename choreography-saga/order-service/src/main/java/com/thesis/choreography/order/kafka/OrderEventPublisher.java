package com.thesis.choreography.order.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.kafka.AbstractEventPublisher;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher extends AbstractEventPublisher {

    private final KafkaTopicsConfig topicsConfig;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                              KafkaTopicsConfig topicsConfig,
                              MeterRegistry meterRegistry) {
        super(kafkaTemplate, topicsConfig, meterRegistry, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.topicsConfig = topicsConfig;
    }

    @Override
    protected String getTopic() {
        return topicsConfig.getOrderEvents();
    }

    @Override
    protected String getServiceName() {
        return SagaMetrics.SERVICE_CHOREOGRAPHY;
    }

    public void publishOrderCreated(OrderCreatedEvent event) {
        publishEvent("OrderCreatedEvent", event, event.getOrderId());
    }
}
