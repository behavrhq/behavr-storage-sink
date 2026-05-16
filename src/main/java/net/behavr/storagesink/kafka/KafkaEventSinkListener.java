package net.behavr.storagesink.kafka;

import java.util.List;
import net.behavr.storagesink.service.EventSinkService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventSinkListener {

	private static final Logger log = LoggerFactory.getLogger(KafkaEventSinkListener.class);

	private final EventSinkService eventSinkService;

	public KafkaEventSinkListener(EventSinkService eventSinkService) {
		this.eventSinkService = eventSinkService;
	}

	@KafkaListener(
			topics = "${behavr.kafka.topic}",
			containerFactory = "batchKafkaListenerContainerFactory")
	public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
		try {
			eventSinkService.processBatch(records);
			ack.acknowledge();
		} catch (RuntimeException ex) {
			log.error("Batch processing failed; offsets will not be committed", ex);
			throw ex;
		}
	}
}
