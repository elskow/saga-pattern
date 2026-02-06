package com.thesis.saga.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.time.Instant;

@Getter
@Setter
@SuperBuilder(toBuilder = true)
public abstract class AbstractSagaContext implements SagaContext {

    private String sagaId;
    private Instant createdAt;
    private Instant lastUpdatedAt;
    private int expectedCompensations;
    private int completedCompensations;

    protected AbstractSagaContext() {
        this.createdAt = Instant.now();
        this.lastUpdatedAt = Instant.now();
        this.expectedCompensations = 0;
        this.completedCompensations = 0;
    }

    protected AbstractSagaContext(String sagaId) {
        this();
        this.sagaId = sagaId;
    }

    @Override
    public void touch() {
        this.lastUpdatedAt = Instant.now();
    }

    @Override
    public void incrementCompletedCompensations() {
        this.completedCompensations++;
    }

    @Override
    @JsonIgnore
    public boolean isCompensationComplete() {
        return completedCompensations >= expectedCompensations;
    }

    @JsonIgnore
    public abstract AbstractSagaContext withTouch();
}
