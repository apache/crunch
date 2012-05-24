/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.types.PType;

/**
 * A {@code Source} represents an input data set that is an input to one
 * or more MapReduce jobs.
 *
 */
public interface Source<T> { 
  /**
   * Returns the {@code PType} for this source.
   */
  PType<T> getType();

  /**
   * Configure the given job to use this source as an input.
   * 
   * @param job The job to configure
   * @param inputId For a multi-input job, an identifier for this input to the job
   * @throws IOException
   */
  void configureSource(Job job, int inputId) throws IOException;

  /**
   * Returns the number of bytes in this {@code Source}.
   */
  long getSize(Configuration configuration);  
}
