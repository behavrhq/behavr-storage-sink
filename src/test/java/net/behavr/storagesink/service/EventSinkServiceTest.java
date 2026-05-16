package net.behavr.storagesink.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import tools.jackson.databind.json.JsonMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import net.behavr.storagesink.config.BehavrSinkProperties;
import net.behavr.storagesink.metrics.SinkMetrics;
import net.behavr.storagesink.partition.EventPartitioner;
import net.behavr.storagesink.partition.ObjectKeyGenerator;
import net.behavr.storagesink.storage.ObjectStorageWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EventSinkServiceTest {

	@Mock
	private ObjectStorageWriter storageWriter;

	@Mock
	private SinkMetrics sinkMetrics;

	private EventSinkService service;
	private BehavrSinkProperties sinkProperties;

	@BeforeEach
	void setUp() {
		sinkProperties = new BehavrSinkProperties();
		sinkProperties.setBucket("behavr-lake");
		sinkProperties.setPrefix("raw/events");
		sinkProperties.setMaxEventsPerFile(1000);
		sinkProperties.setFlushInterval(Duration.ofHours(1));
		sinkProperties.setMaxBufferBytes(10_000_000);
		var mapper = JsonMapper.builder().build();
		var clock = Clock.fixed(Instant.parse("2026-05-11T20:00:00Z"), ZoneOffset.UTC);
		service = new EventSinkService(
				mapper,
				new EventPartitioner(),
				new ObjectKeyGenerator(sinkProperties),
				storageWriter,
				sinkProperties,
				sinkMetrics,
				clock);
	}

	@Test
	void skipsMalformedAndWritesValidNdjson() throws Exception {
		String valid =
				"{\"eventId\":\"e1\",\"eventType\":\"search\",\"siteId\":\"s1\",\"occurredAt\":\"2026-05-11T20:01:00Z\",\"receivedAt\":\"2026-05-11T20:01:01Z\"}";
		var records = new ArrayList<ConsumerRecord<String, String>>();
		records.add(new ConsumerRecord<>("behavr.events.raw", 0, 0L, "s1:e1", "not-json"));
		records.add(new ConsumerRecord<>("behavr.events.raw", 0, 1L, "s1:e1", valid));

		service.processBatch(records);

		verify(sinkMetrics, atLeastOnce()).incrementMalformed();
		verify(storageWriter, atLeastOnce()).write(eq("behavr-lake"), any(), any(), any(), anyInt());
		var contentCaptor = ArgumentCaptor.forClass(byte[].class);
		verify(storageWriter).write(eq("behavr-lake"), any(), contentCaptor.capture(), any(), eq(1));
		assertThat(new String(contentCaptor.getValue())).isEqualTo(valid);
	}

	@Test
	void splitsWhenExceedingMaxEvents() {
		sinkProperties.setMaxEventsPerFile(2);
		sinkProperties.setFlushInterval(Duration.ofHours(24));
		var mapper = JsonMapper.builder().build();
		var clock = Clock.fixed(Instant.parse("2026-05-11T20:00:00Z"), ZoneOffset.UTC);
		service = new EventSinkService(
				mapper,
				new EventPartitioner(),
				new ObjectKeyGenerator(sinkProperties),
				storageWriter,
				sinkProperties,
				sinkMetrics,
				clock);

		var json =
				"{\"eventId\":\"%s\",\"eventType\":\"t\",\"siteId\":\"s\",\"occurredAt\":\"2026-05-11T20:01:00Z\",\"receivedAt\":\"2026-05-11T20:01:01Z\"}";
		List<ConsumerRecord<String, String>> records = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			records.add(new ConsumerRecord<>("t", 0, i, "k", json.formatted("id" + i)));
		}
		service.processBatch(records);

		verify(storageWriter, org.mockito.Mockito.times(3)).write(eq("behavr-lake"), any(), any(), any(), anyInt());
	}
}
