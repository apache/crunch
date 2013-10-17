/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.crunch.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionUtilsTest {

  @Mock
  private PCollection<String> pcollection;

  @Mock
  private Pipeline pipeline;

  @Test
  public void testBasic() throws Exception {
    Configuration conf = new Configuration();
    when(pcollection.getSize()).thenReturn(7 * 1000L * 1000L * 1000L);
    when(pcollection.getPipeline()).thenReturn(pipeline);
    when(pipeline.getConfiguration()).thenReturn(conf);
    assertEquals(8, PartitionUtils.getRecommendedPartitions(pcollection));
  }

  @Test
  public void testBytesPerTask() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(PartitionUtils.BYTES_PER_REDUCE_TASK, 500L * 1000L * 1000L);
    when(pcollection.getSize()).thenReturn(7 * 1000L * 1000L * 1000L);
    when(pcollection.getPipeline()).thenReturn(pipeline);
    when(pipeline.getConfiguration()).thenReturn(conf);
    assertEquals(15, PartitionUtils.getRecommendedPartitions(pcollection));
  }

  @Test
  public void testDefaultMaxRecommended() throws Exception {
    Configuration conf = new Configuration();
    when(pcollection.getSize()).thenReturn(1000 * 1000L * 1000L * 1000L);
    when(pcollection.getPipeline()).thenReturn(pipeline);
    when(pipeline.getConfiguration()).thenReturn(conf);
    assertEquals(500, PartitionUtils.getRecommendedPartitions(pcollection));
  }

  @Test
  public void testMaxRecommended() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(PartitionUtils.MAX_REDUCERS, 400);
    when(pcollection.getSize()).thenReturn(1000 * 1000L * 1000L * 1000L);
    when(pcollection.getPipeline()).thenReturn(pipeline);
    when(pipeline.getConfiguration()).thenReturn(conf);
    assertEquals(400, PartitionUtils.getRecommendedPartitions(pcollection));
  }

  @Test
  public void testNegativeMaxRecommended() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(PartitionUtils.MAX_REDUCERS, -1);
    when(pcollection.getSize()).thenReturn(1000 * 1000L * 1000L * 1000L);
    when(pcollection.getPipeline()).thenReturn(pipeline);
    when(pipeline.getConfiguration()).thenReturn(conf);
    assertEquals(1001, PartitionUtils.getRecommendedPartitions(pcollection));
  }
}
