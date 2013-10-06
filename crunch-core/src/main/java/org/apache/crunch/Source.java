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

import java.io.IOException;

import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * A {@code Source} represents an input data set that is an input to one or more
 * MapReduce jobs.
 * 
 */
public interface Source<T> {

  /**
   * Adds the given key-value pair to the {@code Configuration} instance that is used to read
   * this {@code Source<T></T>}. Allows for multiple inputs to re-use the same config keys with
   * different values when necessary.
   */
  Source<T> inputConf(String key, String value);

  /**
   * Returns the {@code PType} for this source.
   */
  PType<T> getType();

  /**
   * Returns the {@code Converter} used for mapping the inputs from this instance
   * into {@code PCollection} or {@code PTable} values.
   */
  Converter<?, ?, ?, ?> getConverter();
  
  /**
   * Configure the given job to use this source as an input.
   * 
   * @param job
   *          The job to configure
   * @param inputId
   *          For a multi-input job, an identifier for this input to the job
   * @throws IOException
   */
  void configureSource(Job job, int inputId) throws IOException;

  /**
   * Returns the number of bytes in this {@code Source}.
   */
  long getSize(Configuration configuration);
  
  /**
   * Returns the time (in milliseconds) that this {@code Source} was most recently
   * modified (e.g., because an input file was edited or new files were added to
   * a directory.)
   */
  long getLastModifiedAt(Configuration configuration);
}
