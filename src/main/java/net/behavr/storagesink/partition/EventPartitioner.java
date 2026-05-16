package net.behavr.storagesink.partition;

import java.time.Instant;
import java.time.ZoneOffset;
import net.behavr.storagesink.model.CollectedEvent;
import org.springframework.stereotype.Component;

@Component
public class EventPartitioner {

	public PartitionKey partition(CollectedEvent event, Instant consumeTime) {
		Instant effective = event.occurredAt();
		if (effective == null) {
			effective = event.receivedAt();
		}
		if (effective == null) {
			effective = consumeTime;
		}
		var zdt = effective.atZone(ZoneOffset.UTC);
		return new PartitionKey(event.siteId(), zdt.toLocalDate(), zdt.getHour());
	}
}
