package com.thesis.orchestration.payment.config;

import com.thesis.orchestration.payment.handler.PaymentCommandHandler;
import io.eventuate.tram.commands.consumer.CommandDispatcher;
import io.eventuate.tram.sagas.participant.SagaCommandDispatcherFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SagaParticipantConfiguration {

    @Bean
    public CommandDispatcher paymentCommandDispatcher(PaymentCommandHandler paymentCommandHandler,
                                                       SagaCommandDispatcherFactory sagaCommandDispatcherFactory) {
        return sagaCommandDispatcherFactory.make("paymentCommandDispatcher", paymentCommandHandler.commandHandlers());
    }
}
