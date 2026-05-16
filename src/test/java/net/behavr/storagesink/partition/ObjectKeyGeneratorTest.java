package net.behavr.storagesink.partition;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import net.behavr.storagesink.config.BehavrSinkProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ObjectKeyGeneratorTest {

	private ObjectKeyGenerator generator;

	@BeforeEach
	void setUp() {
		var sink = new BehavrSinkProperties();
		sink.setPrefix("raw/events");
		generator = new ObjectKeyGenerator(sink);
	}

	@Test
	void generatesHiveStylePath() {
		var pk = new PartitionKey("site_123", LocalDate.of(2026, 5, 11), 20);
		var key = generator.generateKey(pk, Instant.parse("2026-05-11T20:05:01Z"));
		assertThat(key)
				.startsWith("raw/events/site_id=site_123/date=2026-05-11/hour=20/events_")
				.endsWith(".jsonl");
		assertThat(key).contains("events_20260511T200501Z_");
	}

	@Test
	void stripsTrailingSlashOnPrefix() {
		var sink = new BehavrSinkProperties();
		sink.setPrefix("raw/events///");
		var gen = new ObjectKeyGenerator(sink);
		var pk = new PartitionKey("a", LocalDate.of(2026, 1, 1), 1);
		var key = gen.generateKey(pk, Instant.parse("2026-01-01T01:02:03Z"));
		assertThat(key).startsWith("raw/events/site_id=a/");
	}
}
