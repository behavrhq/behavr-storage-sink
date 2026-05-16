package net.behavr.storagesink.partition;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import net.behavr.storagesink.config.BehavrSinkProperties;
import org.springframework.stereotype.Component;

@Component
public class ObjectKeyGenerator {

	private static final DateTimeFormatter FILE_TS =
			DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

	private final BehavrSinkProperties sinkProperties;

	public ObjectKeyGenerator(BehavrSinkProperties sinkProperties) {
		this.sinkProperties = sinkProperties;
	}

	public String generateKey(PartitionKey partition, Instant fileTimestamp) {
		String prefix = sinkProperties.getPrefix().replaceAll("/+$", "");
		String site = partition.siteId();
		String date = partition.date().toString();
		String hour = String.format("%02d", partition.hour());
		String ts = FILE_TS.format(fileTimestamp);
		String shortId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
		return "%s/site_id=%s/date=%s/hour=%s/events_%s_%s.jsonl"
				.formatted(prefix, site, date, hour, ts, shortId);
	}
}
