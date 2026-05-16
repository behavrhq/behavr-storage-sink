package net.behavr.storagesink.storage;

import java.util.Map;

public interface ObjectStorageWriter {

	void write(String bucket, String key, byte[] content, Map<String, String> metadata, int eventCount);
}
