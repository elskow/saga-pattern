package com.thesis.common.exception;

import lombok.Getter;

/**
 * Base exception for resources that cannot be found.
 */
@Getter
public class ResourceNotFoundException extends RuntimeException {

    private final String resourceType;
    private final String resourceId;

    public ResourceNotFoundException(String resourceType, String resourceId) {
        super(String.format("%s not found: %s", resourceType, resourceId));
        this.resourceType = resourceType;
        this.resourceId = resourceId;
    }

    public ResourceNotFoundException(String resourceType, String resourceId, Throwable cause) {
        super(String.format("%s not found: %s", resourceType, resourceId), cause);
        this.resourceType = resourceType;
        this.resourceId = resourceId;
    }

}
