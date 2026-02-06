package com.thesis.common.metrics;

public final class SagaMetrics {

    public static final String TAG_SERVICE = "service";
    public static final String TAG_STEP = "step";
    public static final String SERVICE_CHOREOGRAPHY = "choreography";
    public static final String SERVICE_ORCHESTRATION = "orchestration";

    public static final String ORDERS_CREATED = "saga.orders.created";
    public static final String ORDERS_COMPLETED = "saga.orders.completed";
    public static final String ORDERS_FAILED = "saga.orders.failed";
    public static final String ORDER_PROCESSING_TIME = "saga.order.processing.time";

    public static final String STEP_PAYMENT_DURATION = "saga.step.payment.duration";
    public static final String STEP_INVENTORY_DURATION = "saga.step.inventory.duration";
    public static final String STEP_SHIPPING_DURATION = "saga.step.shipping.duration";
    public static final String SAGA_TOTAL_DURATION = "saga.total.duration";

    public static final String PAYMENTS_SUCCESS = "payments.success";
    public static final String PAYMENTS_FAILED = "payments.failed";
    public static final String PAYMENT_PROCESSING_TIME = "payment.processing.time";

    public static final String INVENTORY_RESERVATIONS_SUCCESS = "inventory.reservations.success";
    public static final String INVENTORY_RESERVATIONS_FAILED = "inventory.reservations.failed";
    public static final String INVENTORY_RESERVATION_TIME = "inventory.reservation.time";

    public static final String SHIPPING_SUCCESS = "shipping.success";
    public static final String SHIPPING_FAILED = "shipping.failed";
    public static final String SHIPPING_PROCESSING_TIME = "shipping.processing.time";

    public static final String COMPENSATIONS_TOTAL = "saga.compensations.total";
    public static final String COMPENSATIONS_PAYMENT = "saga.compensations.payment";
    public static final String COMPENSATIONS_INVENTORY = "saga.compensations.inventory";
    public static final String COMPENSATIONS_SHIPPING = "saga.compensations.shipping";
    public static final String COMPENSATION_DURATION = "saga.compensation.duration";

    public static final String SAGA_STEPS_EXECUTED = "saga.steps.executed";
    public static final String SAGA_STEPS_FAILED = "saga.steps.failed";

    public static final String COMMANDS_RETRY_ATTEMPTS = "saga.commands.retry.attempts";
    public static final String COMMANDS_RETRY_SKIPPED = "saga.commands.retry.skipped";

    public static final String OUTBOX_PUBLISH_ATTEMPTS = "saga.outbox.publish.attempts";
    public static final String OUTBOX_PUBLISH_SUCCESS = "saga.outbox.publish.success";
    public static final String OUTBOX_PUBLISH_FAILURE = "saga.outbox.publish.failure";
    public static final String OUTBOX_CLEANUP_DELETIONS = "saga.outbox.cleanup.deletions";
    public static final String OUTBOX_MAX_ATTEMPTS_EXCEEDED = "saga.outbox.max.attempts.exceeded";
    public static final String OUTBOX_PENDING_COUNT = "saga.outbox.pending.count";
    public static final String OUTBOX_FAILED_COUNT = "saga.outbox.failed.count";

    public static final String STEP_PAYMENT = "payment";
    public static final String STEP_INVENTORY = "inventory";
    public static final String STEP_SHIPPING = "shipping";

    public static final String SAGA_MESSAGES_TOTAL = "saga.messages.total";
    public static final String SAGA_DB_WRITES_TOTAL = "saga.db.writes.total";
    public static final String SAGA_MESSAGE_LATENCY = "saga.message.latency";
    public static final String SAGA_MESSAGES_PER_TRANSACTION = "saga.messages.per.transaction";
    public static final String SAGA_DB_WRITES_PER_TRANSACTION = "saga.db.writes.per.transaction";

    public static final String TAG_DIRECTION = "direction";
    public static final String DIRECTION_SENT = "sent";
    public static final String DIRECTION_RECEIVED = "received";

    public static final String TAG_MESSAGE_TYPE = "type";
    public static final String TYPE_EVENT = "event";
    public static final String TYPE_COMMAND = "command";
    public static final String TYPE_REPLY = "reply";

    public static final String TAG_OPERATION = "operation";
    public static final String OPERATION_INSERT = "insert";
    public static final String OPERATION_UPDATE = "update";

    public static final String TAG_ENTITY = "entity";
    public static final String ENTITY_ORDER = "order";
    public static final String ENTITY_PAYMENT = "payment";
    public static final String ENTITY_INVENTORY = "inventory";
    public static final String ENTITY_SHIPMENT = "shipment";

    public static final String TAG_OUTCOME = "outcome";
    public static final String OUTCOME_SUCCESS = "success";
    public static final String OUTCOME_FAILURE = "failure";

    public static final String TAG_FROM_SERVICE = "from_service";
    public static final String TAG_TO_SERVICE = "to_service";

    public static final String KAFKA_EVENT_SEND_FAILURE = "saga.kafka.event.send.failure";
    public static final String KAFKA_EVENT_SEND_SUCCESS = "saga.kafka.event.send.success";
    public static final String TAG_EVENT_TYPE = "event_type";
    public static final String KAFKA_PUBLISH_LATENCY = "saga.kafka.publish.latency";
    public static final String KAFKA_CONSUMER_LAG = "saga.kafka.consumer.lag";
    public static final String KAFKA_MESSAGE_AGE = "saga.kafka.message.age";

    public static final String RETRY_ATTEMPTS = "saga.retry.attempts";
    public static final String RETRY_EXHAUSTED = "saga.retry.exhausted";
    public static final String DEADLETTER_COUNT = "saga.deadletter.count";

    public static final String TAG_RETRY_REASON = "reason";
    public static final String RETRY_REASON_OPTIMISTIC_LOCK = "optimistic_lock";
    public static final String RETRY_REASON_TRANSIENT_ERROR = "transient_error";
    public static final String RETRY_REASON_KAFKA_ERROR = "kafka_error";

    public static final String TAG_METHOD = "method";
    public static final String TAG_TOPIC = "topic";

    public static final String IDEMPOTENCY_CHECK_COUNT = "saga.idempotency.check.count";
    public static final String IDEMPOTENCY_DUPLICATE_DETECTED = "saga.idempotency.duplicate.detected";
    public static final String IDEMPOTENCY_TABLE_SIZE = "saga.idempotency.table.size";

    private SagaMetrics() {
    }
}
