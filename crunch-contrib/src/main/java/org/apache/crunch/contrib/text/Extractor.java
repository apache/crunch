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
package org.apache.crunch.contrib.text;

import java.io.Serializable;

import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

/**
 * An interface for extracting a specific data type from a text string that
 * is being processed by a {@code Scanner} object.
 *
 * @param <T> The data type to be extracted
 */
public interface Extractor<T> extends Serializable {
  
  /**
   * Extract a value with the type of this instance.
   */
  T extract(String input);
  
  /**
   * Returns the {@code PType} associated with this data type for the
   * given {@code PTypeFamily}.
   */
  PType<T> getPType(PTypeFamily ptf);
  
  /**
   * Returns the default value for this {@code Extractor} in case of an
   * error.
   */
  T getDefaultValue();
  
  /**
   * Perform any initialization required by this {@code Extractor} during the
   * start of a map or reduce task.
   */
  void initialize();
  
  /**
   * Returns true if the last call to {@code extract} on this instance
   * threw an exception that was handled.
   */
  boolean errorOnLastRecord();
  
  /**
   * Return statistics about how many errors this {@code Extractor} instance
   * encountered while parsing input data.
   */
  ExtractorStats getStats();
}
