package org.apache.crunch.kafka.offset;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Map;

/**
 * Base implementation of {@link OffsetWriter}
 */
public abstract class AbstractOffsetWriter implements OffsetWriter {

  @Override
  public void write(Map<TopicPartition, Long> offsets) throws IOException {
    write(System.currentTimeMillis(), offsets);
  }
}
