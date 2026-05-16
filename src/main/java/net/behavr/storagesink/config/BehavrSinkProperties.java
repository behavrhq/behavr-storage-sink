package net.behavr.storagesink.config;

import java.time.Duration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "behavr.sink")
public class BehavrSinkProperties {

	private String bucket = "behavr-lake";
	private String prefix = "raw/events";
	private int maxEventsPerFile = 1000;
	private Duration flushInterval = Duration.ofSeconds(10);
	private long maxBufferBytes = 5_242_880;

}
