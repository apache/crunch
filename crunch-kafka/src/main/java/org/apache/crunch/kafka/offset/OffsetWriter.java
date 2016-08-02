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
import java.util.Map;

/**
 * Writer for persisting offset information.
 */
public interface OffsetWriter extends Closeable {

  /**
   * Persists the {@code offsets} to a configured location with the current time specified as the as of time.
   *
   * @param offsets the offsets to persist
   * @throws IllegalArgumentException if the {@code offsets} are {@code null}.
   * @throws IOException              if there is an error persisting the offsets.
   */
  void write(Map<TopicPartition, Long> offsets) throws IOException;

  /**
   * Persists the {@code offsets} to a configured location with metadata of {@code asOfTime} indicating
   * the time in milliseconds when the offsets were meaningful.
   *
   * @param asOfTime the metadata describing when the offsets are accurate as of a time given in milliseconds
   *                 since epoch.
   * @param offsets  the offsets to persist
   * @throws IllegalArgumentException if the {@code offsets} are {@code null} or the {@code asOfTime} is less than 0.
   * @throws IOException              if there is an error persisting the offsets.
   */
  void write(long asOfTime, Map<TopicPartition, Long> offsets) throws IOException;
}
