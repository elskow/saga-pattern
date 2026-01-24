package com.thesis.choreography.shipping.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.events.ShippingCancelledEvent;
import com.thesis.common.events.ShippingFailedEvent;
import com.thesis.common.events.ShippingScheduledEvent;
import com.thesis.common.kafka.AbstractEventPublisher;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ShippingEventPublisher extends AbstractEventPublisher {

    private final KafkaTopicsConfig topicsConfig;

    public ShippingEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                  KafkaTopicsConfig topicsConfig,
                                  MeterRegistry meterRegistry) {
        super(kafkaTemplate, topicsConfig, meterRegistry, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.topicsConfig = topicsConfig;
    }

    @Override
    protected String getTopic() {
        return topicsConfig.getShippingEvents();
    }

    @Override
    protected String getServiceName() {
        return SagaMetrics.SERVICE_CHOREOGRAPHY;
    }

    public void publishShippingScheduled(ShippingScheduledEvent event) {
        publishEvent("ShippingScheduledEvent", event, event.getOrderId());
    }

    public void publishShippingFailed(ShippingFailedEvent event) {
        publishEvent("ShippingFailedEvent", event, event.getOrderId());
    }

    public void publishShippingCancelled(ShippingCancelledEvent event) {
        publishEvent("ShippingCancelledEvent", event, event.getOrderId());
    }
}
