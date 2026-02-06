package com.thesis.saga.core;

import java.time.Instant;

public interface SagaContext {

    String getSagaId();

    Instant getCreatedAt();

    Instant getLastUpdatedAt();

    void touch();

    int getExpectedCompensations();

    int getCompletedCompensations();

    void setExpectedCompensations(int count);

    void incrementCompletedCompensations();

    default boolean isCompensationComplete() {
        return getCompletedCompensations() >= getExpectedCompensations();
    }
}
