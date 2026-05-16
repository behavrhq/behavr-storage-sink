package net.behavr.storagesink.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "behavr.kafka")
public class BehavrKafkaProperties {

	private String topic = "behavr.events.raw";

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
}
