package net.behavr.storagesink.partition;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import net.behavr.storagesink.model.CollectedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EventPartitionerTest {

	private EventPartitioner partitioner;

	@BeforeEach
	void setUp() {
		partitioner = new EventPartitioner();
	}

	@Test
	void usesOccurredAtInUtc() {
		var occurred = Instant.parse("2026-05-11T20:05:01Z");
		var event = new CollectedEvent(
				"e1",
				"page_view",
				"site_123",
				null,
				null,
				occurred,
				Instant.parse("2026-05-11T21:00:00Z"),
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
		PartitionKey pk = partitioner.partition(event, Instant.parse("2026-05-12T00:00:00Z"));
		assertThat(pk.siteId()).isEqualTo("site_123");
		assertThat(pk.date()).isEqualTo(LocalDate.of(2026, 5, 11));
		assertThat(pk.hour()).isEqualTo(20);
	}

	@Test
	void fallsBackToReceivedAt() {
		var received = Instant.parse("2026-01-02T03:04:05Z");
		var event = new CollectedEvent(
				"e1",
				"t",
				"s",
				null,
				null,
				null,
				received,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
		PartitionKey pk = partitioner.partition(event, Instant.EPOCH);
		assertThat(pk.date()).isEqualTo(LocalDate.of(2026, 1, 2));
		assertThat(pk.hour()).isEqualTo(3);
	}

	@Test
	void fallsBackToConsumeTime() {
		var consume = Instant.parse("2026-07-15T14:30:00Z");
		var event = new CollectedEvent(
				"e1",
				"t",
				"s",
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
		PartitionKey pk = partitioner.partition(event, consume);
		assertThat(pk.date()).isEqualTo(LocalDate.of(2026, 7, 15));
		assertThat(pk.hour()).isEqualTo(14);
	}
}
