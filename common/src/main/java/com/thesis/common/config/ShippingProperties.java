package com.thesis.common.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "app.shipping")
@Validated
public record ShippingProperties(
    @Min(1) int estimatedDeliveryDays,
    @NotBlank String trackingNumberPrefix
) {
    public ShippingProperties {
        if (estimatedDeliveryDays <= 0) estimatedDeliveryDays = 3;
        if (trackingNumberPrefix == null || trackingNumberPrefix.isBlank()) trackingNumberPrefix = "TRK-";
    }

    public ShippingProperties() {
        this(3, "TRK-");
    }
}
