package com.thesis.orchestration.shipping.config;

import com.thesis.orchestration.shipping.handler.ShippingCommandHandler;
import io.eventuate.tram.commands.consumer.CommandDispatcher;
import io.eventuate.tram.sagas.participant.SagaCommandDispatcherFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SagaParticipantConfiguration {

    @Bean
    public CommandDispatcher shippingCommandDispatcher(ShippingCommandHandler shippingCommandHandler,
                                                        SagaCommandDispatcherFactory sagaCommandDispatcherFactory) {
        return sagaCommandDispatcherFactory.make("shippingCommandDispatcher", shippingCommandHandler.commandHandlers());
    }
}
