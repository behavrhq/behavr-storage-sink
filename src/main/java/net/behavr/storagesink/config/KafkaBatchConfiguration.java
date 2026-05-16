package net.behavr.storagesink.config;

import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaBatchConfiguration {

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory(
			ConsumerFactory<String, String> consumerFactory, KafkaProperties kafkaProperties) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
		factory.setConsumerFactory(consumerFactory);
		factory.setBatchListener(true);
		Integer concurrency = kafkaProperties.getListener().getConcurrency();
		if (concurrency != null && concurrency > 0) {
			factory.setConcurrency(concurrency);
		}
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		return factory;
	}
}
