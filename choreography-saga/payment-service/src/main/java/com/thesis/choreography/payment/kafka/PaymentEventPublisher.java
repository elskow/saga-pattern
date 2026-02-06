package com.thesis.choreography.payment.kafka;

import com.thesis.common.config.KafkaTopicsProperties;
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

    public PaymentEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                 KafkaTopicsProperties topics,
                                 MeterRegistry meterRegistry) {
        super(kafkaTemplate, topics, meterRegistry);
    }

    @Override
    protected String getTopic() {
        return topics().paymentEvents();
    }

    @Override
    protected String getServiceName() {
        return SagaMetrics.SERVICE_CHOREOGRAPHY;
    }

    public void publishPaymentCompleted(PaymentCompletedEvent event) {
        publishEvent("PaymentCompletedEvent", event, event.orderId());
    }

    public void publishPaymentFailed(PaymentFailedEvent event) {
        publishEvent("PaymentFailedEvent", event, event.orderId());
    }

    public void publishPaymentRefunded(PaymentRefundedEvent event) {
        publishEvent("PaymentRefundedEvent", event, event.orderId());
    }
}
