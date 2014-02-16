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
package org.apache.crunch.types;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

/**
 * Performs deep copies of values.
 * 
 * @param <T> The type of value that will be copied
 */
public interface DeepCopier<T> extends Serializable {

  /**
   * Initialize the deep copier with a job-specific configuration
   * 
   * @param conf Job-specific configuration
   */
  void initialize(Configuration conf);

  /**
   * Create a deep copy of a value.
   * 
   * @param source The value to be copied
   * @return The deep copy of the value
   */
  T deepCopy(T source);

}
