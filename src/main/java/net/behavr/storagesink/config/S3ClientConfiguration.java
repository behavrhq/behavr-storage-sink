package net.behavr.storagesink.config;

import java.net.URI;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

@Configuration
public class S3ClientConfiguration {

	@Bean
	public S3Client s3Client(BehavrStorageProperties props) {
		var builder = S3Client.builder().region(Region.of(props.getRegion()));
		if (StringUtils.hasText(props.getEndpoint())) {
			builder.endpointOverride(URI.create(props.getEndpoint()));
		}
		builder.serviceConfiguration(
				S3Configuration.builder().pathStyleAccessEnabled(props.isPathStyleAccess()).build());
		if (StringUtils.hasText(props.getAccessKey()) && StringUtils.hasText(props.getSecretKey())) {
			builder.credentialsProvider(StaticCredentialsProvider.create(
					AwsBasicCredentials.create(props.getAccessKey(), props.getSecretKey())));
		} else {
			builder.credentialsProvider(DefaultCredentialsProvider.create());
		}
		return builder.build();
	}
}
