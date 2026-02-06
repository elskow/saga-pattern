package com.thesis.choreography.shipping.kafka;

import com.thesis.common.config.KafkaTopicsProperties;
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

    public ShippingEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                  KafkaTopicsProperties topics,
                                  MeterRegistry meterRegistry) {
        super(kafkaTemplate, topics, meterRegistry);
    }

    @Override
    protected String getTopic() {
        return topics().shippingEvents();
    }

    @Override
    protected String getServiceName() {
        return SagaMetrics.SERVICE_CHOREOGRAPHY;
    }

    public void publishShippingScheduled(ShippingScheduledEvent event) {
        publishEvent("ShippingScheduledEvent", event, event.orderId());
    }

    public void publishShippingFailed(ShippingFailedEvent event) {
        publishEvent("ShippingFailedEvent", event, event.orderId());
    }

    public void publishShippingCancelled(ShippingCancelledEvent event) {
        publishEvent("ShippingCancelledEvent", event, event.orderId());
    }
}
