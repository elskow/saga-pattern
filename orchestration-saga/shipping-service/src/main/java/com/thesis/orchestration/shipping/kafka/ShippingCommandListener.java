package com.thesis.orchestration.shipping.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.CancelShippingCommand;
import com.thesis.common.command.ScheduleShippingCommand;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.ShippingFailedReply;
import com.thesis.common.replies.ShippingScheduledReply;
import com.thesis.orchestration.shipping.model.ShipmentEntity;
import com.thesis.orchestration.shipping.repository.ShipmentRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka-based command listener for shipping service.
 * Handles shipping scheduling and cancellation commands from the saga orchestrator.
 */
@Component
@Slf4j
public class ShippingCommandListener {

    private static final String COMMAND_TOPIC = "orchestration.shipping.commands";
    private static final String REPLY_TOPIC = "orchestration.shipping.replies";

    private final ShipmentRepository shipmentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Counter shippingSuccessCounter;
    private final Counter shippingFailedCounter;
    private final Timer shippingProcessingTimer;
    private final Counter compensationShippingCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final SagaMetricsHelper metricsHelper;

    public ShippingCommandListener(ShipmentRepository shipmentRepository,
                                    KafkaTemplate<String, Object> kafkaTemplate,
                                    ObjectMapper objectMapper,
                                    MeterRegistry meterRegistry) {
        this.shipmentRepository = shipmentRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.shippingSuccessCounter = meterRegistry.counter(SagaMetrics.SHIPPING_SUCCESS,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.shippingFailedCounter = meterRegistry.counter(SagaMetrics.SHIPPING_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.shippingProcessingTimer = meterRegistry.timer(SagaMetrics.STEP_SHIPPING_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationShippingCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_SHIPPING,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationDurationTimer = meterRegistry.timer(SagaMetrics.COMPENSATION_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.sagaStepsExecutedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_EXECUTED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.sagaStepsFailedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "shipping-service")
    public void handleCommand(String message) {
        try {
            // Try to parse as ScheduleShippingCommand first
            if (message.contains("shippingAddress")) {
                ScheduleShippingCommand command = objectMapper.readValue(message, ScheduleShippingCommand.class);
                handleScheduleShipping(command);
                return;
            }

            // Try to parse as CancelShippingCommand
            if (message.contains("shipmentId") && message.contains("orderId")) {
                CancelShippingCommand command = objectMapper.readValue(message, CancelShippingCommand.class);
                handleCancelShipping(command);
            }
        } catch (Exception e) {
            log.error("Failed to process command: {}", message, e);
        }
    }

    @Observed(name = "shipping.schedule", contextualName = "schedule-shipping")
    private void handleScheduleShipping(ScheduleShippingCommand command) {
        shippingProcessingTimer.record(() -> {
            log.info("Scheduling shipping {} for order {} to address: {}",
                    command.getShipmentId(), command.getOrderId(), command.getShippingAddress());

            metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

            try {
                String trackingNumber = "TRK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

                ShipmentEntity shipment = ShipmentEntity.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .shippingAddress(command.getShippingAddress())
                        .trackingNumber(trackingNumber)
                        .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                        .scheduledAt(Instant.now())
                        .build();
                shipmentRepository.save(shipment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_SHIPMENT);

                shippingSuccessCounter.increment();
                sagaStepsExecutedCounter.increment();

                ShippingScheduledReply reply = ShippingScheduledReply.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .build();

                kafkaTemplate.send(REPLY_TOPIC, command.getOrderId(), reply);
                metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
                log.info("Shipping {} scheduled successfully with tracking: {}", command.getShipmentId(), trackingNumber);

            } catch (Exception e) {
                ShipmentEntity shipment = ShipmentEntity.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .shippingAddress(command.getShippingAddress())
                        .status(ShipmentEntity.ShipmentStatus.FAILED)
                        .failureReason(e.getMessage())
                        .build();
                shipmentRepository.save(shipment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_SHIPMENT);

                shippingFailedCounter.increment();
                sagaStepsFailedCounter.increment();

                ShippingFailedReply reply = ShippingFailedReply.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .reason("Shipping scheduling failed: " + e.getMessage())
                        .build();

                kafkaTemplate.send(REPLY_TOPIC, command.getOrderId(), reply);
                metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
                log.error("Shipping {} scheduling failed: {}", command.getShipmentId(), e.getMessage());
            }
        });
    }

    @Observed(name = "shipping.cancel", contextualName = "cancel-shipping")
    private void handleCancelShipping(CancelShippingCommand command) {
        long startTime = System.currentTimeMillis();
        log.info("Cancelling shipping {} for order {}", command.getShipmentId(), command.getOrderId());

        metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

        shipmentRepository.findById(command.getShipmentId()).ifPresent(shipment -> {
            shipment.setStatus(ShipmentEntity.ShipmentStatus.CANCELLED);
            shipment.setCancelledAt(Instant.now());
            shipment.setCancellationReason("Order cancelled - saga compensation");
            shipmentRepository.save(shipment);
            metricsHelper.recordDbUpdate(command.getOrderId(), SagaMetrics.ENTITY_SHIPMENT);
            log.info("Shipping {} cancelled successfully", command.getShipmentId());
        });

        compensationShippingCounter.increment();
        compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
        metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
    }
}
