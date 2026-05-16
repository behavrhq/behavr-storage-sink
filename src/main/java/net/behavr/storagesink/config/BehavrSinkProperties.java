package net.behavr.storagesink.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "behavr.sink")
public class BehavrSinkProperties {

	private String bucket = "behavr-lake";
	private String prefix = "raw/events";
	private int maxEventsPerFile = 1000;
	private Duration flushInterval = Duration.ofSeconds(10);
	private long maxBufferBytes = 5_242_880;

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public int getMaxEventsPerFile() {
		return maxEventsPerFile;
	}

	public void setMaxEventsPerFile(int maxEventsPerFile) {
		this.maxEventsPerFile = maxEventsPerFile;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(Duration flushInterval) {
		this.flushInterval = flushInterval;
	}

	public long getMaxBufferBytes() {
		return maxBufferBytes;
	}

	public void setMaxBufferBytes(long maxBufferBytes) {
		this.maxBufferBytes = maxBufferBytes;
	}
}
