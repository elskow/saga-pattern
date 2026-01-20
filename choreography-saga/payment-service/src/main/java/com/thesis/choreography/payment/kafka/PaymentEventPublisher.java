package com.thesis.choreography.payment.kafka;

import com.thesis.common.dto.KafkaTopics;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Publishing PaymentCompletedEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(KafkaTopics.PAYMENT_EVENTS, event.getOrderId(), event);
    }

    public void publishPaymentFailed(PaymentFailedEvent event) {
        log.info("Publishing PaymentFailedEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(KafkaTopics.PAYMENT_EVENTS, event.getOrderId(), event);
    }

    public void publishPaymentRefunded(PaymentRefundedEvent event) {
        log.info("Publishing PaymentRefundedEvent for order: {}", event.getOrderId());
        kafkaTemplate.send(KafkaTopics.PAYMENT_EVENTS, event.getOrderId(), event);
    }
}
