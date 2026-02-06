package com.thesis.common.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
    KafkaTopicsProperties.class,
    KafkaErrorHandlingProperties.class,
    PaymentProperties.class,
    ShippingProperties.class
})
public class KafkaTopicsConfiguration {
}
