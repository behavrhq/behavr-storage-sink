package net.behavr.storagesink.service;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.behavr.storagesink.config.BehavrSinkProperties;
import net.behavr.storagesink.metrics.SinkMetrics;
import net.behavr.storagesink.model.CollectedEvent;
import net.behavr.storagesink.partition.EventPartitioner;
import net.behavr.storagesink.partition.ObjectKeyGenerator;
import net.behavr.storagesink.partition.PartitionKey;
import net.behavr.storagesink.storage.ObjectStorageWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class EventSinkService {

	private static final Logger log = LoggerFactory.getLogger(EventSinkService.class);

	private final ObjectMapper objectMapper;
	private final EventPartitioner partitioner;
	private final ObjectKeyGenerator keyGenerator;
	private final ObjectStorageWriter storageWriter;
	private final BehavrSinkProperties sinkProperties;
	private final SinkMetrics sinkMetrics;
	private final Clock clock;

	public EventSinkService(
			ObjectMapper objectMapper,
			EventPartitioner partitioner,
			ObjectKeyGenerator keyGenerator,
			ObjectStorageWriter storageWriter,
			BehavrSinkProperties sinkProperties,
			SinkMetrics sinkMetrics,
			Clock clock) {
		this.objectMapper = objectMapper;
		this.partitioner = partitioner;
		this.keyGenerator = keyGenerator;
		this.storageWriter = storageWriter;
		this.sinkProperties = sinkProperties;
		this.sinkMetrics = sinkMetrics;
		this.clock = clock;
	}

	public void processBatch(List<ConsumerRecord<String, String>> records) {
		if (records == null || records.isEmpty()) {
			return;
		}
		log.info("Processing Kafka batch size={}", records.size());
		int malformedInBatch = 0;
		Map<String, LineBuffer> buffers = new HashMap<>();

		for (ConsumerRecord<String, String> record : records) {
			sinkMetrics.incrementConsumed();
			String raw = record.value();
			if (raw == null || raw.isBlank()) {
				sinkMetrics.incrementMalformed();
				malformedInBatch++;
				continue;
			}
			CollectedEvent event;
			try {
				event = objectMapper.readValue(raw, CollectedEvent.class);
			} catch (JacksonException ex) {
				log.warn("Malformed JSON in Kafka record partition={} offset={}: {}", record.partition(), record.offset(), ex.getMessage());
				sinkMetrics.incrementMalformed();
				malformedInBatch++;
				continue;
			}
			if (!isValid(event)) {
				log.warn(
						"Skipping invalid event partition={} offset={} event_id={}",
						record.partition(),
						record.offset(),
						event != null ? event.eventId() : null);
				sinkMetrics.incrementMalformed();
				malformedInBatch++;
				continue;
			}

			Instant consumeTime = Instant.now(clock);
			PartitionKey pk = partitioner.partition(event, consumeTime);
			LineBuffer buf = buffers.computeIfAbsent(pk.groupingKey(), k -> new LineBuffer(pk));
			buf.addLine(raw, consumeTime);

			flushWhileOverCapacity(buf);
		}

		for (LineBuffer buf : buffers.values()) {
			flushRemainder(buf);
		}

		if (malformedInBatch > 0) {
			log.info("Batch completed with malformed skipped count={}", malformedInBatch);
		}
	}

	private void flushWhileOverCapacity(LineBuffer buf) {
		int maxEvents = sinkProperties.getMaxEventsPerFile();
		long maxBytes = sinkProperties.getMaxBufferBytes();
		Duration flushInterval = sinkProperties.getFlushInterval();
		while (!buf.isEmpty()) {
			Instant now = Instant.now(clock);
			if (!buf.shouldFlush(maxEvents, maxBytes, flushInterval, now)) {
				break;
			}
			int chunk = buf.computeFlushChunk(maxEvents, maxBytes, flushInterval, now);
			if (chunk <= 0) {
				break;
			}
			flushBufferChunk(buf, chunk, now);
		}
	}

	private void flushRemainder(LineBuffer buf) {
		int maxEvents = sinkProperties.getMaxEventsPerFile();
		long maxBytes = sinkProperties.getMaxBufferBytes();
		while (!buf.isEmpty()) {
			Instant now = Instant.now(clock);
			int n = Math.min(buf.lineCount(), maxEvents);
			if (ndjsonSize(buf.viewLines(), n) > maxBytes) {
				n = Math.min(n, buf.linesCountForByteLimit(maxBytes));
			}
			n = Math.max(1, Math.min(n, buf.lineCount()));
			flushBufferChunk(buf, n, now);
		}
	}

	private void flushBufferChunk(LineBuffer buf, int lineCount, Instant now) {
		List<String> snapshot = buf.removeFirst(lineCount, now);
		if (snapshot.isEmpty()) {
			return;
		}
		byte[] bytes = joinNdjson(snapshot);
		PartitionKey pk = buf.partitionKey;
		String objectKey = keyGenerator.generateKey(pk, Instant.now(clock));
		Map<String, String> meta = Map.of(
				"service", "behavr-storage-sink",
				"format", "jsonl",
				"event_count", Integer.toString(snapshot.size()));
		try {
			storageWriter.write(sinkProperties.getBucket(), objectKey, bytes, meta, snapshot.size());
		} catch (RuntimeException ex) {
			sinkMetrics.incrementWriteErrors();
			throw ex;
		}
		sinkMetrics.incrementFilesWritten();
		sinkMetrics.incrementWritten(snapshot.size());
	}

	private static long ndjsonSize(List<String> lines, int firstN) {
		if (firstN <= 0) {
			return 0;
		}
		long sum = 0;
		for (int i = 0; i < firstN; i++) {
			sum += lines.get(i).getBytes(StandardCharsets.UTF_8).length;
		}
		return sum + (firstN - 1L);
	}

	private static int linesCountForByteLimit(List<String> lines, long maxBytes) {
		long acc = 0;
		int n = 0;
		for (String line : lines) {
			long len = line.getBytes(StandardCharsets.UTF_8).length;
			long withSep = acc + len + (n > 0 ? 1 : 0);
			if (n == 0 && len > maxBytes) {
				return 1;
			}
			if (n > 0 && withSep > maxBytes) {
				break;
			}
			n++;
			acc = withSep;
		}
		return Math.max(1, n);
	}

	private static byte[] joinNdjson(List<String> lines) {
		if (lines.isEmpty()) {
			return new byte[0];
		}
		int total = 0;
		for (String line : lines) {
			total += line.getBytes(StandardCharsets.UTF_8).length;
		}
		total += lines.size() - 1;
		var out = new byte[total];
		int pos = 0;
		for (int i = 0; i < lines.size(); i++) {
			if (i > 0) {
				out[pos++] = '\n';
			}
			byte[] chunk = lines.get(i).getBytes(StandardCharsets.UTF_8);
			System.arraycopy(chunk, 0, out, pos, chunk.length);
			pos += chunk.length;
		}
		return out;
	}

	private static boolean isValid(CollectedEvent e) {
		if (e == null) {
			return false;
		}
		if (!StringUtils.hasText(e.eventId()) || !StringUtils.hasText(e.eventType()) || !StringUtils.hasText(e.siteId())) {
			return false;
		}
		return e.occurredAt() != null || e.receivedAt() != null;
	}

	private final class LineBuffer {

		private final PartitionKey partitionKey;
		private final List<String> lines = new ArrayList<>();
		private long wireBytes;
		private Instant bufferStartedAt;

		private LineBuffer(PartitionKey partitionKey) {
			this.partitionKey = partitionKey;
		}

		private int lineCount() {
			return lines.size();
		}

		private List<String> viewLines() {
			return lines;
		}

		private int linesCountForByteLimit(long maxBytes) {
			return EventSinkService.linesCountForByteLimit(lines, maxBytes);
		}

		private void addLine(String rawJson, Instant now) {
			if (lines.isEmpty()) {
				bufferStartedAt = now;
			}
			wireBytes += rawJson.getBytes(StandardCharsets.UTF_8).length;
			lines.add(rawJson);
		}

		private boolean isEmpty() {
			return lines.isEmpty();
		}

		private long ndjsonSize() {
			if (lines.isEmpty()) {
				return 0;
			}
			return wireBytes + (lines.size() - 1L);
		}

		private boolean shouldFlush(int maxEvents, long maxBytes, Duration flushInterval, Instant now) {
			if (lines.size() >= maxEvents) {
				return true;
			}
			if (ndjsonSize() >= maxBytes) {
				return true;
			}
			return bufferStartedAt != null && !bufferStartedAt.plus(flushInterval).isAfter(now);
		}

		private int computeFlushChunk(int maxEvents, long maxBytes, Duration flushInterval, Instant now) {
			if (lines.size() >= maxEvents) {
				return maxEvents;
			}
			if (ndjsonSize() >= maxBytes) {
				return linesCountForByteLimit(maxBytes);
			}
			if (bufferStartedAt != null && !bufferStartedAt.plus(flushInterval).isAfter(now)) {
				return lines.size();
			}
			return 0;
		}

		private List<String> removeFirst(int n, Instant now) {
			int take = Math.min(n, lines.size());
			var sub = new ArrayList<>(lines.subList(0, take));
			for (int i = 0; i < take; i++) {
				wireBytes -= lines.get(i).getBytes(StandardCharsets.UTF_8).length;
			}
			lines.subList(0, take).clear();
			if (lines.isEmpty()) {
				bufferStartedAt = null;
			} else {
				bufferStartedAt = now;
			}
			return sub;
		}
	}
}
