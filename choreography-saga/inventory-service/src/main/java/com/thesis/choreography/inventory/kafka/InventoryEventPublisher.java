package com.thesis.choreography.inventory.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.events.InventoryReleasedEvent;
import com.thesis.common.events.InventoryReservationFailedEvent;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.kafka.AbstractEventPublisher;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventPublisher extends AbstractEventPublisher {

    private final KafkaTopicsConfig topicsConfig;

    public InventoryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaTopicsConfig topicsConfig,
                                   MeterRegistry meterRegistry) {
        super(kafkaTemplate, topicsConfig, meterRegistry, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.topicsConfig = topicsConfig;
    }

    @Override
    protected String getTopic() {
        return topicsConfig.getInventoryEvents();
    }

    @Override
    protected String getServiceName() {
        return SagaMetrics.SERVICE_CHOREOGRAPHY;
    }

    public void publishInventoryReserved(InventoryReservedEvent event) {
        publishEvent("InventoryReservedEvent", event, event.getOrderId());
    }

    public void publishInventoryReservationFailed(InventoryReservationFailedEvent event) {
        publishEvent("InventoryReservationFailedEvent", event, event.getOrderId());
    }

    public void publishInventoryReleased(InventoryReleasedEvent event) {
        publishEvent("InventoryReleasedEvent", event, event.getOrderId());
    }
}
