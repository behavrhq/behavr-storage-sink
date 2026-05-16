package net.behavr.storagesink.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({BehavrKafkaProperties.class, BehavrSinkProperties.class, BehavrStorageProperties.class})
public class BehavrConfiguration {}
