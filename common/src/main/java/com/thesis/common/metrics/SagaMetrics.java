package com.thesis.common.metrics;

/**
 * Centralized metric name constants for saga pattern implementations.
 * This ensures consistent metric naming across choreography and orchestration patterns.
 * 
 * All metrics use the tag "service" with values "choreography" or "orchestration"
 * to differentiate between the two saga pattern implementations.
 */
public final class SagaMetrics {

    private SagaMetrics() {
        // Utility class - prevent instantiation
    }

    // ========== SERVICE TAGS ==========
    public static final String TAG_SERVICE = "service";
    public static final String TAG_STEP = "step";
    public static final String SERVICE_CHOREOGRAPHY = "choreography";
    public static final String SERVICE_ORCHESTRATION = "orchestration";

    // ========== ORDER METRICS ==========
    /** Counter: Number of orders created */
    public static final String ORDERS_CREATED = "saga.orders.created";
    /** Counter: Number of orders successfully completed */
    public static final String ORDERS_COMPLETED = "saga.orders.completed";
    /** Counter: Number of orders failed */
    public static final String ORDERS_FAILED = "saga.orders.failed";
    /** Timer: End-to-end order processing time (from creation to completion/failure) */
    public static final String ORDER_PROCESSING_TIME = "saga.order.processing.time";

    // ========== SAGA STEP DURATION METRICS ==========
    /** Timer: Payment step duration */
    public static final String STEP_PAYMENT_DURATION = "saga.step.payment.duration";
    /** Timer: Inventory reservation step duration */
    public static final String STEP_INVENTORY_DURATION = "saga.step.inventory.duration";
    /** Timer: Shipping scheduling step duration */
    public static final String STEP_SHIPPING_DURATION = "saga.step.shipping.duration";
    /** Timer: Total saga duration (all steps combined) */
    public static final String SAGA_TOTAL_DURATION = "saga.total.duration";

    // ========== SERVICE-LEVEL METRICS (existing, for compatibility) ==========
    /** Counter: Successful payments */
    public static final String PAYMENTS_SUCCESS = "payments.success";
    /** Counter: Failed payments */
    public static final String PAYMENTS_FAILED = "payments.failed";
    /** Timer: Payment processing time */
    public static final String PAYMENT_PROCESSING_TIME = "payment.processing.time";

    /** Counter: Successful inventory reservations */
    public static final String INVENTORY_RESERVATIONS_SUCCESS = "inventory.reservations.success";
    /** Counter: Failed inventory reservations */
    public static final String INVENTORY_RESERVATIONS_FAILED = "inventory.reservations.failed";
    /** Timer: Inventory reservation time */
    public static final String INVENTORY_RESERVATION_TIME = "inventory.reservation.time";

    /** Counter: Successful shipping */
    public static final String SHIPPING_SUCCESS = "shipping.success";
    /** Counter: Failed shipping */
    public static final String SHIPPING_FAILED = "shipping.failed";
    /** Timer: Shipping processing time */
    public static final String SHIPPING_PROCESSING_TIME = "shipping.processing.time";

    // ========== COMPENSATION METRICS ==========
    /** Counter: Total number of saga compensations triggered */
    public static final String COMPENSATIONS_TOTAL = "saga.compensations.total";
    /** Counter: Payment refund compensations */
    public static final String COMPENSATIONS_PAYMENT = "saga.compensations.payment";
    /** Counter: Inventory release compensations */
    public static final String COMPENSATIONS_INVENTORY = "saga.compensations.inventory";
    /** Counter: Shipping cancellation compensations */
    public static final String COMPENSATIONS_SHIPPING = "saga.compensations.shipping";
    /** Timer: Time to complete compensation */
    public static final String COMPENSATION_DURATION = "saga.compensation.duration";

    // ========== SAGA STATE METRICS ==========
    /** Counter: Saga steps executed */
    public static final String SAGA_STEPS_EXECUTED = "saga.steps.executed";
    /** Counter: Saga steps failed */
    public static final String SAGA_STEPS_FAILED = "saga.steps.failed";

    // ========== COMMAND RETRY METRICS ==========
    /** Counter: Retry attempts for pending commands */
    public static final String COMMANDS_RETRY_ATTEMPTS = "saga.commands.retry.attempts";
    /** Counter: Retry skipped due to terminal saga */
    public static final String COMMANDS_RETRY_SKIPPED = "saga.commands.retry.skipped";

    // ========== OUTBOX METRICS ==========
    /** Counter: Outbox publish attempts */
    public static final String OUTBOX_PUBLISH_ATTEMPTS = "saga.outbox.publish.attempts";
    /** Counter: Outbox publish successes */
    public static final String OUTBOX_PUBLISH_SUCCESS = "saga.outbox.publish.success";
    /** Counter: Outbox publish failures */
    public static final String OUTBOX_PUBLISH_FAILURE = "saga.outbox.publish.failure";
    /** Counter: Outbox cleanup deletions */
    public static final String OUTBOX_CLEANUP_DELETIONS = "saga.outbox.cleanup.deletions";
    /** Counter: Outbox entries exceeding max attempts */
    public static final String OUTBOX_MAX_ATTEMPTS_EXCEEDED = "saga.outbox.max.attempts.exceeded";
    /** Gauge: Pending outbox count */
    public static final String OUTBOX_PENDING_COUNT = "saga.outbox.pending.count";
    /** Gauge: Failed outbox count */
    public static final String OUTBOX_FAILED_COUNT = "saga.outbox.failed.count";

    // ========== STEP NAMES (for tagging) ==========
    public static final String STEP_PAYMENT = "payment";
    public static final String STEP_INVENTORY = "inventory";
    public static final String STEP_SHIPPING = "shipping";

    // ========== THESIS COMPARISON METRICS ==========
    // These metrics are specifically designed for comparing choreography vs orchestration patterns

    /** 
     * Counter: Number of messages exchanged per saga transaction.
     * Tags: service (choreography/orchestration), direction (sent/received), type (event/command/reply)
     * This helps measure communication overhead between the two patterns.
     */
    public static final String SAGA_MESSAGES_TOTAL = "saga.messages.total";
    
    /**
     * Counter: Number of database write operations per saga transaction.
     * Tags: service (choreography/orchestration), operation (insert/update), entity (order/payment/inventory/shipment)
     * This helps measure data persistence overhead.
     */
    public static final String SAGA_DB_WRITES_TOTAL = "saga.db.writes.total";
    
    /**
     * Timer: Latency of message delivery between services.
     * Tags: service (choreography/orchestration), from_service, to_service
     * Measures time from message publish to message receive.
     */
    public static final String SAGA_MESSAGE_LATENCY = "saga.message.latency";
    
    /**
     * Distribution Summary: Messages per completed saga.
     * Tags: service (choreography/orchestration), outcome (success/failure)
     * Records the total message count when a saga completes.
     */
    public static final String SAGA_MESSAGES_PER_TRANSACTION = "saga.messages.per.transaction";
    
    /**
     * Distribution Summary: DB writes per completed saga.
     * Tags: service (choreography/orchestration), outcome (success/failure)
     * Records the total DB write count when a saga completes.
     */
    public static final String SAGA_DB_WRITES_PER_TRANSACTION = "saga.db.writes.per.transaction";

    // ========== MESSAGE DIRECTION TAGS ==========
    public static final String TAG_DIRECTION = "direction";
    public static final String DIRECTION_SENT = "sent";
    public static final String DIRECTION_RECEIVED = "received";
    
    // ========== MESSAGE TYPE TAGS ==========
    public static final String TAG_MESSAGE_TYPE = "type";
    public static final String TYPE_EVENT = "event";
    public static final String TYPE_COMMAND = "command";
    public static final String TYPE_REPLY = "reply";
    
    // ========== DB OPERATION TAGS ==========
    public static final String TAG_OPERATION = "operation";
    public static final String OPERATION_INSERT = "insert";
    public static final String OPERATION_UPDATE = "update";
    
    // ========== ENTITY TAGS ==========
    public static final String TAG_ENTITY = "entity";
    public static final String ENTITY_ORDER = "order";
    public static final String ENTITY_PAYMENT = "payment";
    public static final String ENTITY_INVENTORY = "inventory";
    public static final String ENTITY_SHIPMENT = "shipment";
    
    // ========== OUTCOME TAGS ==========
    public static final String TAG_OUTCOME = "outcome";
    public static final String OUTCOME_SUCCESS = "success";
    public static final String OUTCOME_FAILURE = "failure";
    
    // ========== SERVICE NAME TAGS (for message routing) ==========
    public static final String TAG_FROM_SERVICE = "from_service";
    public static final String TAG_TO_SERVICE = "to_service";
    
    // ========== KAFKA SEND FAILURE METRICS ==========
    /** Counter: Kafka event send failures */
    public static final String KAFKA_EVENT_SEND_FAILURE = "saga.kafka.event.send.failure";
    /** Counter: Kafka event send successes */
    public static final String KAFKA_EVENT_SEND_SUCCESS = "saga.kafka.event.send.success";
    /** Tag: Event type for Kafka send metrics */
    public static final String TAG_EVENT_TYPE = "event_type";
}
