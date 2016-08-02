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
package org.apache.crunch.kafka.offset;

import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Reader API that supports reading offset information from an underlying storage mechanism.
 */
public interface OffsetReader extends Closeable {

  /**
   * Reads the last stored offsets.
   *
   * @return the last stored offsets.  If there are no stored offsets an empty collection will be returned.
   * @throws IOException if there is an error reading from the underlying storage.
   */
  Map<TopicPartition, Long> readLatestOffsets() throws IOException;

  /**
   * Reads the offsets for a given {@code persistedOffsetTime}.  Note that not all storage mechanisms support
   * complete historical offset information.  Use the {@link #getStoredOffsetPersistenceTimes()} to find valid values
   * to specify for {@code persistedOffsetTime}.
   *
   * @param persistedOffsetTime the persistence time when offsets were written to the underlying storage system.
   * @return returns the offsets persisted at the specified {@code persistedOffsetTime}.  If no offsets were persisted
   * at that time or available to be retrieved then {@code null} will be returned.
   * @throws IOException if there is an error reading from the underlying storage.
   */
  Map<TopicPartition, Long> readOffsets(long persistedOffsetTime) throws IOException;

  /**
   * Returns the list of available persistence times offsets have been written to the underlying storage mechanism.
   * The list of available persistence times will be returned in the order of earliest to latest.
   *
   * @return the collection of persistence times in the form of milliseconds since epoch.  If there are no historical
   * persistence times then an {@code empty list} is returned.
   * @throws IOException if there is an error reading from the underlying storage.
   */
  public List<Long> getStoredOffsetPersistenceTimes() throws IOException;
}
