package net.behavr.storagesink.storage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(MockitoExtension.class)
class S3ObjectStorageWriterTest {

	@Mock
	private S3Client s3Client;

	@InjectMocks
	private S3ObjectStorageWriter writer;

	@Test
	void retriesOnS3Failure() {
		when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
				.thenThrow(S3Exception.builder().message("throttle").build())
				.thenReturn(PutObjectResponse.builder().build());
		writer.write("b", "k", new byte[] {1, 2}, Map.of("service", "behavr-storage-sink"), 1);
		verify(s3Client, times(2)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
	}

	@Test
	void failsAfterMaxAttempts() {
		when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
				.thenThrow(S3Exception.builder().message("err").build());
		assertThatThrownBy(() -> writer.write("b", "k", new byte[] {1}, Map.of(), 1))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("S3 put failed");
		verify(s3Client, times(3)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
	}
}
