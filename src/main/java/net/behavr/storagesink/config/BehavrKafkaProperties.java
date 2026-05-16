package net.behavr.storagesink.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "behavr.kafka")
public class BehavrKafkaProperties {

	private String topic = "behavr.events.raw";

}
