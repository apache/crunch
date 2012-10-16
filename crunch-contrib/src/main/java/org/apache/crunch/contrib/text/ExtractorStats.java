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

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Records the number of kind of errors that an {@code Extractor} encountered when parsing
 * input data.
 */
public class ExtractorStats {

  private final int errorCount;
  private final List<Integer> fieldErrors;
  
  public ExtractorStats(int errorCount) {
    this(errorCount, ImmutableList.<Integer>of());
  }
  
  public ExtractorStats(int errorCount, List<Integer> fieldErrors) {
    this.errorCount = errorCount;
    this.fieldErrors = fieldErrors;
  }
  
  /**
   * The overall number of records that had some kind of parsing error.
   * @return The overall number of records that had some kind of parsing error 
   */
  public int getErrorCount() {
    return errorCount;
  }
  
  /**
   * Returns the number of errors that occurred when parsing the individual fields of
   * a composite record type, like a {@code Pair} or {@code TupleN}.
   * @return The number of errors that occurred when parsing the individual fields of
   * a composite record type
   */
  public List<Integer> getFieldErrors() {
    return fieldErrors;
  }
}
