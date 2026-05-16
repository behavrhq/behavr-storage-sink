package net.behavr.storagesink.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class SinkMetrics {

	private final Counter recordsConsumed;
	private final Counter recordsWritten;
	private final Counter recordsMalformed;
	private final Counter filesWritten;
	private final Counter writeErrors;

	public SinkMetrics(MeterRegistry registry) {
		this.recordsConsumed =
				Counter.builder("behavr_storage_sink_records_consumed").register(registry);
		this.recordsWritten =
				Counter.builder("behavr_storage_sink_records_written").register(registry);
		this.recordsMalformed =
				Counter.builder("behavr_storage_sink_records_malformed").register(registry);
		this.filesWritten = Counter.builder("behavr_storage_sink_files_written").register(registry);
		this.writeErrors = Counter.builder("behavr_storage_sink_write_errors").register(registry);
	}

	public void incrementConsumed() {
		recordsConsumed.increment();
	}

	public void incrementWritten(int n) {
		recordsWritten.increment(n);
	}

	public void incrementMalformed() {
		recordsMalformed.increment();
	}

	public void incrementFilesWritten() {
		filesWritten.increment();
	}

	public void incrementWriteErrors() {
		writeErrors.increment();
	}
}
