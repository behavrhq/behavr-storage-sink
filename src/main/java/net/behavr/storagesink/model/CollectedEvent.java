package net.behavr.storagesink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CollectedEvent(
		String eventId,
		String eventType,
		String siteId,
		String anonymousId,
		String sessionId,
		Instant occurredAt,
		Instant receivedAt,
		Instant batchSentAt,
		String url,
		String path,
		String title,
		String referrer,
		String userAgent,
		String browserLanguage,
		String deviceType,
		String sdkVersion,
		Map<String, Object> utm,
		Map<String, Object> properties,
		ServerContextPayload serverContext) {

	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ServerContextPayload(String ipAddress, String userAgent, String requestId) {}
}
