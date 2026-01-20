package com.thesis.orchestration.inventory.config;

import com.thesis.orchestration.inventory.handler.InventoryCommandHandler;
import io.eventuate.tram.commands.consumer.CommandDispatcher;
import io.eventuate.tram.sagas.participant.SagaCommandDispatcherFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SagaParticipantConfiguration {

    @Bean
    public CommandDispatcher inventoryCommandDispatcher(InventoryCommandHandler inventoryCommandHandler,
                                                         SagaCommandDispatcherFactory sagaCommandDispatcherFactory) {
        return sagaCommandDispatcherFactory.make("inventoryCommandDispatcher", inventoryCommandHandler.commandHandlers());
    }
}
