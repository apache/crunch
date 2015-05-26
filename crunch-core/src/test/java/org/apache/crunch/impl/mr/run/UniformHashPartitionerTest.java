/*
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

package org.apache.crunch.impl.mr.run;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.junit.Test;

public class UniformHashPartitionerTest {

  private static final UniformHashPartitioner INSTANCE = new UniformHashPartitioner();

  // Simple test to ensure that the general idea behind this partitioner is working.
  // We create 100 keys that have exactly the same lower-order bits, and partition them into 10 buckets,
  // and then verify that every bucket got at least 20% of the keys. The default HashPartitioner would put
  // everything in the same bucket.
  @Test
  public void testGetPartition() {
    Multiset<Integer> partitionCounts = HashMultiset.create();
    final int NUM_PARTITIONS = 10;
    for (int i = 0; i < 1000; i += 10) {
      partitionCounts.add(INSTANCE.getPartition(i, i, NUM_PARTITIONS));
    }
    for (int partitionNumber = 0; partitionNumber < NUM_PARTITIONS; partitionNumber++) {
      assertThat(partitionCounts.count(partitionNumber), greaterThan(5));
    }
  }
}