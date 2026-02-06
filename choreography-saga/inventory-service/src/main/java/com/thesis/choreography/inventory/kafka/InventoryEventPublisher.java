package com.thesis.choreography.inventory.kafka;

import com.thesis.common.config.KafkaTopicsProperties;
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

    public InventoryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaTopicsProperties topics,
                                   MeterRegistry meterRegistry) {
        super(kafkaTemplate, topics, meterRegistry);
    }

    @Override
    protected String getTopic() {
        return topics().inventoryEvents();
    }

    @Override
    protected String getServiceName() {
        return SagaMetrics.SERVICE_CHOREOGRAPHY;
    }

    public void publishInventoryReserved(InventoryReservedEvent event) {
        publishEvent("InventoryReservedEvent", event, event.orderId());
    }

    public void publishInventoryReservationFailed(InventoryReservationFailedEvent event) {
        publishEvent("InventoryReservationFailedEvent", event, event.orderId());
    }

    public void publishInventoryReleased(InventoryReleasedEvent event) {
        publishEvent("InventoryReleasedEvent", event, event.orderId());
    }
}
