package net.behavr.storagesink.partition;

import java.time.LocalDate;

public record PartitionKey(String siteId, LocalDate date, int hour) {

	public String groupingKey() {
		return siteId + "|" + date + "|" + hour;
	}
}
