package com.thesis.orchestration.shipping.handler;

import com.thesis.common.command.CancelShippingCommand;
import com.thesis.common.command.ScheduleShippingCommand;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.ShippingFailedReply;
import com.thesis.common.replies.ShippingScheduledReply;
import com.thesis.orchestration.shipping.model.ShipmentEntity;
import com.thesis.orchestration.shipping.repository.ShipmentRepository;
import io.eventuate.tram.commands.consumer.CommandHandlers;
import io.eventuate.tram.commands.consumer.CommandMessage;
import io.eventuate.tram.messaging.common.Message;
import io.eventuate.tram.sagas.participant.SagaCommandHandlersBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

import static io.eventuate.tram.commands.consumer.CommandHandlerReplyBuilder.withFailure;
import static io.eventuate.tram.commands.consumer.CommandHandlerReplyBuilder.withSuccess;

@Component
@Slf4j
public class ShippingCommandHandler {

    private final ShipmentRepository shipmentRepository;
    private final Counter shippingSuccessCounter;
    private final Counter shippingFailedCounter;
    private final Timer shippingProcessingTimer;
    private final Counter compensationShippingCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final SagaMetricsHelper metricsHelper;

    public ShippingCommandHandler(ShipmentRepository shipmentRepository, MeterRegistry meterRegistry) {
        this.shipmentRepository = shipmentRepository;
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

    public CommandHandlers commandHandlers() {
        return SagaCommandHandlersBuilder
            .fromChannel("shipping-service")
            .onMessage(ScheduleShippingCommand.class, this::handleScheduleShipping)
            .onMessage(CancelShippingCommand.class, this::handleCancelShipping)
            .build();
    }

    @Observed(name = "shipping.schedule", contextualName = "schedule-shipping")
    private Message handleScheduleShipping(CommandMessage<ScheduleShippingCommand> cm) {
        return shippingProcessingTimer.record(() -> {
            ScheduleShippingCommand command = cm.getCommand();
            log.info("Scheduling shipping {} for order {} to address: {}",
                command.getShipmentId(), command.getOrderId(), command.getShippingAddress());

            // Record command received
            metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

            try {
                // Generate tracking number
                String trackingNumber = "TRK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

                // Create shipment entity
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
                metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
                log.info("Shipping {} scheduled successfully with tracking: {}", command.getShipmentId(), trackingNumber);
                return withSuccess(ShippingScheduledReply.builder()
                    .shipmentId(command.getShipmentId())
                    .orderId(command.getOrderId())
                    .build());
            } catch (Exception e) {
                // Create failed shipment record
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
                metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
                log.error("Shipping {} scheduling failed: {}", command.getShipmentId(), e.getMessage());
                return withFailure(ShippingFailedReply.builder()
                    .shipmentId(command.getShipmentId())
                    .orderId(command.getOrderId())
                    .reason("Shipping scheduling failed: " + e.getMessage())
                    .build());
            }
        });
    }

    @Observed(name = "shipping.cancel", contextualName = "cancel-shipping")
    private Message handleCancelShipping(CommandMessage<CancelShippingCommand> cm) {
        long startTime = System.currentTimeMillis();
        CancelShippingCommand command = cm.getCommand();
        log.info("Cancelling shipping {} for order {}", command.getShipmentId(), command.getOrderId());

        // Record command received
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
        
        return withSuccess();
    }

    public SagaMetricsHelper getMetricsHelper() {
        return metricsHelper;
    }
}
