package net.behavr.storagesink.storage;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Component
public class S3ObjectStorageWriter implements ObjectStorageWriter {

	private static final Logger log = LoggerFactory.getLogger(S3ObjectStorageWriter.class);
	private static final String CONTENT_TYPE = "application/x-ndjson";
	private static final int MAX_ATTEMPTS = 3;

	private final S3Client s3Client;

	public S3ObjectStorageWriter(S3Client s3Client) {
		this.s3Client = s3Client;
	}

	@Override
	public void write(String bucket, String key, byte[] content, Map<String, String> metadata, int eventCount) {
		var requestBuilder = PutObjectRequest.builder()
				.bucket(bucket)
				.key(key)
				.contentType(CONTENT_TYPE)
				.metadata(metadata);

		Exception last = null;
		for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
			try {
				s3Client.putObject(requestBuilder.build(), RequestBody.fromBytes(content));
				log.info("Wrote object s3://{}/{} ({} bytes, {} events)", bucket, key, content.length, eventCount);
				return;
			} catch (S3Exception ex) {
				last = ex;
				log.warn("S3 put attempt {} failed for {}: {}", attempt, key, ex.getMessage());
				if (attempt < MAX_ATTEMPTS) {
					sleepBackoff(attempt);
				}
			}
		}
		throw new IllegalStateException("S3 put failed after " + MAX_ATTEMPTS + " attempts for " + key, last);
	}

	private static void sleepBackoff(int attempt) {
		try {
			Thread.sleep(200L * attempt);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(ie);
		}
	}
}
