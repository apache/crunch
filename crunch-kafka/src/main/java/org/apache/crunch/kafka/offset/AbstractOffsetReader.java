package org.apache.crunch.kafka.offset;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Base implementation of {@link OffsetReader}
 */
public abstract class AbstractOffsetReader implements OffsetReader {

  @Override
  public Map<TopicPartition, Long> readOffsets(long persistedOffsetTime) throws IOException {
    throw new UnsupportedOperationException("Operation to read old offsets is not supported");
  }

  @Override
  public List<Long> getStoredOffsetPersistenceTimes() throws IOException {
    throw new UnsupportedOperationException("Operation to retrieve old offset persistence times is not supported");
  }

}
