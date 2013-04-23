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
package org.apache.crunch.impl.mr.exec;

/**
 * Generate a series of capped numbers exponentially.
 *
 * It is used for creating retry intervals. It is NOT thread-safe.
 */
public class CappedExponentialCounter {

  private long current;
  private final long limit;

  public CappedExponentialCounter(long start, long limit) {
    this.current = start;
    this.limit = limit;
  }

  public long get() {
    long result = current;
    current = Math.min(current * 2, limit);
    return result;
  }
}
