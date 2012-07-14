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
package org.apache.crunch.test;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

/**
 * A utility class used during unit testing to update and read counters.
 */
public class TestCounters {

  private static Counters COUNTERS = new Counters();

  public static Counter getCounter(Enum<?> e) {
    return COUNTERS.findCounter(e);
  }

  public static Counter getCounter(String group, String name) {
    return COUNTERS.findCounter(group, name);
  }

  public static void clearCounters() {
    COUNTERS = new Counters();
  }
}
