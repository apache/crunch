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
package org.apache.crunch.lib;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;

/**
 * Utilities for controlling how the data in a {@code PCollection} is balanced across reducers
 * and output files.
 */
public class Shard {

  /**
   * Creates a {@code PCollection<T>} that has the same contents as its input argument but will
   * be written to a fixed number of output files. This is useful for map-only jobs that process
   * lots of input files but only write out a small amount of input per task.
   * 
   * @param pc The {@code PCollection<T>} to rebalance
   * @param numPartitions The number of output partitions to create
   * @return A rebalanced {@code PCollection<T>} with the same contents as the input
   */
  public static <T> PCollection<T> shard(PCollection<T> pc, int numPartitions) {
    return pc.by(new ShardFn<T>(), pc.getTypeFamily().ints())
        .groupByKey(numPartitions)
        .ungroup()
        .values();
  }
  
  private static class ShardFn<T> extends MapFn<T, Integer> {

    private int count;

    @Override
    public void initialize() {
      count = 0;
    }

    @Override
    public Integer map(T input) {
      return count++;
    }
  }
}
