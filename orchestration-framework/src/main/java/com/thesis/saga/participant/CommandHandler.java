package com.thesis.saga.participant;

@FunctionalInterface
public interface CommandHandler<C, R> {

    R handle(C command) throws Exception;
}
