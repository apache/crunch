/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.kafka.inputformat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.kafka.common.TopicPartition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * InputSplit that represent retrieving data from a single {@link TopicPartition} between the specified start
 * and end offsets.
 */
public class KafkaInputSplit extends InputSplit implements Writable {

  private long startingOffset;
  private long endingOffset;
  private TopicPartition topicPartition;

  /**
   * Nullary Constructor for creating the instance inside the Mapper instance.
   */
  public KafkaInputSplit() {

  }

  /**
   * Constructs an input split for the provided {@code topic} and {@code partition} restricting data to be between
   * the {@code startingOffset} and {@code endingOffset}
   * @param topic the topic for the split
   * @param partition the partition for the topic
   * @param startingOffset the start of the split
   * @param endingOffset the end of the split
   */
  public KafkaInputSplit(String topic, int partition, long startingOffset, long endingOffset) {
    this.startingOffset = startingOffset;
    this.endingOffset = endingOffset;
    topicPartition = new TopicPartition(topic, partition);
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    // This is just used as a hint for size of bytes so it is already inaccurate.
    return startingOffset > 0 ? endingOffset - startingOffset : endingOffset;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    //Leave empty since data locality not really an issue.
    return new String[0];
  }

  /**
   * Returns the topic and partition for the split
   * @return the topic and partition for the split
   */
  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  /**
   * Returns the starting offset for the split
   * @return the starting offset for the split
   */
  public long getStartingOffset() {
    return startingOffset;
  }

  /**
   * Returns the ending offset for the split
   * @return the ending offset for the split
   */
  public long getEndingOffset() {
    return endingOffset;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(topicPartition.topic());
    dataOutput.writeInt(topicPartition.partition());
    dataOutput.writeLong(startingOffset);
    dataOutput.writeLong(endingOffset);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    String topic = dataInput.readUTF();
    int partition = dataInput.readInt();
    startingOffset = dataInput.readLong();
    endingOffset = dataInput.readLong();

    topicPartition = new TopicPartition(topic, partition);
  }

  @Override
  public String toString() {
    return getTopicPartition() + " Start: " + startingOffset + " End: " + endingOffset;
  }
}
