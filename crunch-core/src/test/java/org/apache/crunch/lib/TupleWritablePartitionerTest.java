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

import static org.junit.Assert.assertEquals;

import org.apache.crunch.lib.join.JoinUtils.TupleWritablePartitioner;
import org.apache.crunch.types.writable.TupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

public class TupleWritablePartitionerTest {

  private TupleWritablePartitioner tupleWritableParitioner;

  @Before
  public void setUp() {
    tupleWritableParitioner = new TupleWritablePartitioner();
  }

  @Test
  public void testGetPartition() {
    IntWritable intWritable = new IntWritable(3);
    TupleWritable key = new TupleWritable(new Writable[] { intWritable });
    assertEquals(3, tupleWritableParitioner.getPartition(key, NullWritable.get(), 5));
    assertEquals(1, tupleWritableParitioner.getPartition(key, NullWritable.get(), 2));
  }

  @Test
  public void testGetPartition_NegativeHashValue() {
    IntWritable intWritable = new IntWritable(-3);
    // Sanity check, if this doesn't work then the premise of this test is wrong
    assertEquals(-3, intWritable.hashCode());

    TupleWritable key = new TupleWritable(new Writable[] { intWritable });
    assertEquals(3, tupleWritableParitioner.getPartition(key, NullWritable.get(), 5));
    assertEquals(1, tupleWritableParitioner.getPartition(key, NullWritable.get(), 2));
  }

  @Test
  public void testGetPartition_IntegerMinValue() {
    IntWritable intWritable = new IntWritable(Integer.MIN_VALUE);
    // Sanity check, if this doesn't work then the premise of this test is wrong
    assertEquals(Integer.MIN_VALUE, intWritable.hashCode());

    TupleWritable key = new TupleWritable(new Writable[] { intWritable });
    assertEquals(0, tupleWritableParitioner.getPartition(key, NullWritable.get(), Integer.MAX_VALUE));
  }

}
