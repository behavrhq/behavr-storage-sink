package net.behavr.storagesink.health;

import net.behavr.storagesink.config.BehavrSinkProperties;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

@Component("objectStorage")
public class ObjectStorageHealthIndicator implements HealthIndicator {

	private final S3Client s3Client;
	private final BehavrSinkProperties sinkProperties;

	public ObjectStorageHealthIndicator(S3Client s3Client, BehavrSinkProperties sinkProperties) {
		this.s3Client = s3Client;
		this.sinkProperties = sinkProperties;
	}

	@Override
	public Health health() {
		String bucket = sinkProperties.getBucket();
		try {
			s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
			return Health.up().withDetail("bucket", bucket).build();
		} catch (Exception ex) {
			return Health.down(ex).withDetail("bucket", bucket).build();
		}
	}
}
