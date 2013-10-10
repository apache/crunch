/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * Represents the contents of a data source that can be read on the cluster from within one
 * of the tasks running as part of a Crunch pipeline.
 */
public interface ReadableData<T> extends Serializable {

  /**
   * @return Any {@code SourceTarget} instances that must exist before the data in
   * this instance can be read. Used by the planner in sequencing job processing.
   */
  Set<SourceTarget<?>> getSourceTargets();

  /**
   * Allows this instance to specify any additional configuration settings that may
   * be needed by the job that it is launched in.
   *
   * @param conf The {@code Configuration} object for the job
   */
  void configure(Configuration conf);

  /**
   * Read the data referenced by this instance within the given context.
   *
   * @param context The context of the task that is reading the data
   * @return An iterable reference to the data in this instance
   * @throws IOException If the data cannot be read
   */
  Iterable<T> read(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException;
}
