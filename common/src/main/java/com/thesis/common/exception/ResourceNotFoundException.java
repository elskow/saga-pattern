package com.thesis.common.exception;

import lombok.Getter;

@Getter
public class ResourceNotFoundException extends RuntimeException {

    private final String resourceType;
    private final String resourceId;

    public ResourceNotFoundException(String resourceType, String resourceId) {
        super("%s not found: %s".formatted(resourceType, resourceId));
        this.resourceType = resourceType;
        this.resourceId = resourceId;
    }

    public ResourceNotFoundException(String resourceType, String resourceId, Throwable cause) {
        super("%s not found: %s".formatted(resourceType, resourceId), cause);
        this.resourceType = resourceType;
        this.resourceId = resourceId;
    }

}
