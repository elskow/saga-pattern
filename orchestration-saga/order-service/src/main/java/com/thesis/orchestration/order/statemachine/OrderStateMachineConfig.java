package com.thesis.orchestration.order.statemachine;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.config.StateMachineBuilder;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;
import org.springframework.statemachine.state.State;

import java.util.EnumSet;

/**
 * Spring State Machine configuration for Order Saga orchestration.
 * Uses programmatic StateMachineBuilder approach for GraalVM native image compatibility.
 *
 * Note: Unlike @EnableStateMachineFactory approach, this creates state machines
 * programmatically at runtime, which is fully AOT-compatible.
 */
@Configuration
@Slf4j
public class OrderStateMachineConfig {

    /**
     * Factory bean for creating order saga state machines.
     * This approach is AOT-compatible because it builds state machines
     * programmatically at runtime rather than using declarative configuration.
     */
    @Bean
    public OrderStateMachineFactory orderStateMachineFactory() {
        return new OrderStateMachineFactory();
    }

    /**
     * Custom factory that creates state machines programmatically.
     * AOT-compatible - no configuration builders serialized at build time.
     */
    public static class OrderStateMachineFactory {

        /**
         * Creates a new state machine instance for the given order ID.
         */
        public StateMachine<OrderStates, OrderEvents> create(String machineId) {
            try {
                StateMachineBuilder.Builder<OrderStates, OrderEvents> builder =
                    StateMachineBuilder.builder();

                // Configure the state machine
                builder.configureConfiguration()
                    .withConfiguration()
                    .machineId(machineId)
                    .autoStartup(false)
                    .listener(createListener());

                // Configure states
                builder.configureStates()
                    .withStates()
                    .initial(OrderStates.CREATED)
                    .states(EnumSet.allOf(OrderStates.class))
                    .end(OrderStates.COMPLETED)
                    .end(OrderStates.CANCELLED);

                // Configure transitions
                configureTransitions(builder);

                return builder.build();

            } catch (Exception e) {
                throw new RuntimeException("Failed to create state machine for order: " + machineId, e);
            }
        }

        private void configureTransitions(StateMachineBuilder.Builder<OrderStates, OrderEvents> builder)
                throws Exception {
            builder.configureTransitions()
                // Forward flow: CREATED -> PAYMENT_PENDING
                .withExternal()
                    .source(OrderStates.CREATED)
                    .target(OrderStates.PAYMENT_PENDING)
                    .event(OrderEvents.START_SAGA)
                .and()

                // Payment success: PAYMENT_PENDING -> INVENTORY_PENDING
                .withExternal()
                    .source(OrderStates.PAYMENT_PENDING)
                    .target(OrderStates.INVENTORY_PENDING)
                    .event(OrderEvents.PAYMENT_SUCCESS)
                .and()

                // Payment failure: PAYMENT_PENDING -> CANCELLED (no compensation needed)
                .withExternal()
                    .source(OrderStates.PAYMENT_PENDING)
                    .target(OrderStates.CANCELLED)
                    .event(OrderEvents.PAYMENT_FAILED)
                .and()

                // Inventory reserved: INVENTORY_PENDING -> SHIPPING_PENDING
                .withExternal()
                    .source(OrderStates.INVENTORY_PENDING)
                    .target(OrderStates.SHIPPING_PENDING)
                    .event(OrderEvents.INVENTORY_RESERVED)
                .and()

                // Inventory failure: INVENTORY_PENDING -> COMPENSATING
                .withExternal()
                    .source(OrderStates.INVENTORY_PENDING)
                    .target(OrderStates.COMPENSATING)
                    .event(OrderEvents.INVENTORY_FAILED)
                .and()

                // Shipping scheduled: SHIPPING_PENDING -> COMPLETED
                .withExternal()
                    .source(OrderStates.SHIPPING_PENDING)
                    .target(OrderStates.COMPLETED)
                    .event(OrderEvents.SHIPPING_SCHEDULED)
                .and()

                // Shipping failure: SHIPPING_PENDING -> COMPENSATING
                .withExternal()
                    .source(OrderStates.SHIPPING_PENDING)
                    .target(OrderStates.COMPENSATING)
                    .event(OrderEvents.SHIPPING_FAILED)
                .and()

                // Compensation complete: COMPENSATING -> CANCELLED
                .withExternal()
                    .source(OrderStates.COMPENSATING)
                    .target(OrderStates.CANCELLED)
                    .event(OrderEvents.COMPENSATION_COMPLETE);
        }

        private StateMachineListenerAdapter<OrderStates, OrderEvents> createListener() {
            return new StateMachineListenerAdapter<>() {
                @Override
                public void stateChanged(State<OrderStates, OrderEvents> from,
                                        State<OrderStates, OrderEvents> to) {
                    // Note: machineId is the orderId
                    if (from != null && to != null) {
                        log.info("State changed from {} to {}", from.getId(), to.getId());
                    } else if (to != null) {
                        log.info("State machine started in state {}", to.getId());
                    }
                }

                @Override
                public void stateMachineStarted(StateMachine<OrderStates, OrderEvents> stateMachine) {
                    String orderId = stateMachine.getId();
                    log.debug("State machine started for orderId: {}", orderId);
                }

                @Override
                public void stateMachineStopped(StateMachine<OrderStates, OrderEvents> stateMachine) {
                    String orderId = stateMachine.getId();
                    log.debug("State machine stopped for orderId: {}", orderId);
                }

                @Override
                public void stateMachineError(StateMachine<OrderStates, OrderEvents> stateMachine, Exception e) {
                    String orderId = stateMachine.getId();
                    log.error("State machine error for orderId: {}: {}", orderId, e.getMessage(), e);
                }
            };
        }
    }
}
