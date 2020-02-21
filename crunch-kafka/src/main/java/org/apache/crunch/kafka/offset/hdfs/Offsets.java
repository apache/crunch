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
package org.apache.crunch.kafka.offset.hdfs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.requests.ListOffsetRequest;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Simple object to represent a collection of Kafka Topic and Partition offset information to make storing
 * this information easier.
 */
@JsonDeserialize(builder = Offsets.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Offsets {

  private final long offsetsAsOfTime;

  private final List<PartitionOffset> offsets;

  private Offsets(long asOfTime, List<PartitionOffset> offsets) {
    offsetsAsOfTime = asOfTime;
    this.offsets = offsets;
  }

  /**
   * Returns the time in milliseconds since epoch that the offset information was retrieved or valid as of.
   *
   * @return the time in milliseconds since epoch that the offset information was retrieved or valid as of.
   */
  @JsonProperty("asOfTime")
  public long getAsOfTime() {
    return offsetsAsOfTime;
  }

  /**
   * The collection of offset information for specific topics and partitions.
   *
   * @return collection of offset information for specific topics and partitions.
   */
  @JsonProperty("offsets")
  public List<PartitionOffset> getOffsets() {
    return offsets;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offsetsAsOfTime, offsets);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (obj instanceof Offsets) {
      Offsets that = (Offsets) obj;

      return this.offsetsAsOfTime == that.offsetsAsOfTime
          && this.offsets.equals(that.offsets);
    }

    return false;
  }


  /**
   * Builder for the {@link Offsets}.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {

    private long asOf = -1;
    private List<PartitionOffset> offsets = Collections.emptyList();

    /**
     * Creates a new Builder instance.
     *
     * @return a new Builder instance.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Sets the as of time for the collection of offsets.
     *
     * @param asOfTime the as of time for the offsets.
     * @return builder instance
     * @throws IllegalArgumentException if the {@code asOfTime} is less than 0.
     */
    @JsonProperty("asOfTime")
    public Builder setAsOfTime(long asOfTime) {
      if (asOfTime < 0) {
        throw new IllegalArgumentException("The 'asOfTime' cannot be less than 0.");
      }
      this.asOf = asOfTime;
      return this;
    }

    /**
     * Sets the collection of offsets.
     *
     * @param offsets the collection of offsets
     * @return builder instance
     * @throws IllegalArgumentException if the {@code offsets} is {@code null}.
     */
    @JsonProperty("offsets")
    public Builder setOffsets(List<PartitionOffset> offsets) {
      if (offsets == null) {
        throw new IllegalArgumentException("The 'offsets' cannot be 'null'.");
      }
      List<PartitionOffset> sortedOffsets = new LinkedList<>(offsets);

      Collections.sort(sortedOffsets);

      this.offsets = Collections.unmodifiableList(sortedOffsets);

      return this;
    }

    /**
     * Builds an instance.
     *
     * @return a built instance
     * @throws IllegalStateException if the {@link #setAsOfTime(long) asOfTime} is not set or the specified
     *                               {@link #setOffsets(List) offsets} contains duplicate entries for a topic partition.
     */
    public Offsets build() {
      if (asOf < 0) {
        throw new IllegalStateException("The 'asOfTime' cannot be less than 0.");
      }

      Set<String> uniqueTopicPartitions = new HashSet<>();
      for(PartitionOffset partitionOffset : offsets){
        uniqueTopicPartitions.add(partitionOffset.getTopic()+partitionOffset.getPartition());
      }

      if (uniqueTopicPartitions.size() != offsets.size()) {
        throw new IllegalStateException("The 'offsets' contains duplicate entries for a topic and partition.");
      }

      return new Offsets(asOf, offsets);
    }
  }


  /**
   * Simple object that represents a specific topic, partition, and its offset value.
   */
  @JsonDeserialize(builder = PartitionOffset.Builder.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class PartitionOffset implements Comparable<PartitionOffset> {

    private final String topic;
    private final int partition;
    private final long offset;

    private PartitionOffset(String topic, int partition, long offset) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }

    /**
     * Returns the topic
     *
     * @return the topic
     */
    public String getTopic() {
      return topic;
    }

    /**
     * Returns the partition
     *
     * @return the partition
     */
    public int getPartition() {
      return partition;
    }

    /**
     * Returns the offset
     *
     * @return the offset
     */
    public long getOffset() {
      return offset;
    }

    @Override
    public int compareTo(PartitionOffset other) {
      int compare = topic.compareTo(other.topic);
      if (compare == 0) {
        compare = Integer.compare(partition, other.partition);
        if (compare == 0) {
          return Long.compare(offset, other.offset);
        }
      }
      return compare;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }

      if (obj instanceof PartitionOffset) {
        PartitionOffset that = (PartitionOffset) obj;

        return compareTo(that) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, partition, offset);
    }

    /**
     * Builder for {@link PartitionOffset}
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder {

      private String topic;
      private int partition = -1;
      private long offset = ListOffsetRequest.EARLIEST_TIMESTAMP;

      /**
       * Creates a new builder instance.
       *
       * @return a new builder instance.
       */
      public static Builder newBuilder() {
        return new Builder();
      }

      /**
       * Set the {@code topic} for the partition offset being built
       *
       * @param topic the topic for the partition offset being built.
       * @return builder instance
       * @throws IllegalArgumentException if the {@code topic} is {@code null} or empty.
       */
      @JsonProperty("topic")
      public Builder setTopic(String topic) {
        if (StringUtils.isBlank(topic)) {
          throw new IllegalArgumentException("The 'topic' cannot be null or empty.");
        }
        this.topic = topic;
        return this;
      }

      /**
       * Set the {@code partition} for the partition offset being built
       *
       * @param partition the partition for the partition offset being built.
       * @return builder instance
       * @throws IllegalArgumentException if the {@code partition} is less than 0.
       */
      @JsonProperty("partition")
      public Builder setPartition(int partition) {
        if (partition < 0) {
          throw new IllegalArgumentException("The 'partition' cannot be less than 0.");
        }
        this.partition = partition;
        return this;
      }

      /**
       * Set the {@code offset} for the partition offset being built.  If the {@code offset} is not
       * set then it defaults to {@link ListOffsetRequest#EARLIEST_TIMESTAMP}.
       *
       * @param offset the topic for the partition offset being built.
       * @return builder instance
       */
      @JsonProperty("offset")
      public Builder setOffset(long offset) {
        this.offset = offset;
        return this;
      }

      /**
       * Builds a PartitionOffset instance.
       *
       * @return the built PartitionOffset instance.
       * @throws IllegalStateException if the {@code topic} or {@code partition} are never set or configured
       *                               to invalid values.
       */
      public PartitionOffset build() {
        if (StringUtils.isBlank(topic)) {
          throw new IllegalStateException("The 'topic' cannot be null or empty.");
        }

        if (partition < 0) {
          throw new IllegalStateException("The 'partition' cannot be less than 0.");
        }

        return new PartitionOffset(topic, partition, offset);
      }
    }
  }
}
