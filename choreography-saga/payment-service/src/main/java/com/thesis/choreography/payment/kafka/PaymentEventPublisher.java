package com.thesis.choreography.payment.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import com.thesis.common.kafka.AbstractEventPublisher;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventPublisher extends AbstractEventPublisher {

    private final KafkaTopicsConfig topicsConfig;

    public PaymentEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                 KafkaTopicsConfig topicsConfig,
                                 MeterRegistry meterRegistry) {
        super(kafkaTemplate, topicsConfig, meterRegistry, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.topicsConfig = topicsConfig;
    }

    @Override
    protected String getTopic() {
        return topicsConfig.getPaymentEvents();
    }

    @Override
    protected String getServiceName() {
        return SagaMetrics.SERVICE_CHOREOGRAPHY;
    }

    public void publishPaymentCompleted(PaymentCompletedEvent event) {
        publishEvent("PaymentCompletedEvent", event, event.getOrderId());
    }

    public void publishPaymentFailed(PaymentFailedEvent event) {
        publishEvent("PaymentFailedEvent", event, event.getOrderId());
    }

    public void publishPaymentRefunded(PaymentRefundedEvent event) {
        publishEvent("PaymentRefundedEvent", event, event.getOrderId());
    }
}
