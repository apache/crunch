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
package org.apache.crunch;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CreateIT {

  @Rule
  public TemporaryPath tmpDir = new TemporaryPath();

  @Test
  public void testMRWritable() throws Exception {
    run(new MRPipeline(CreateIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance());
  }

  @Test
  public void testMRAvro() throws Exception {
    run(new MRPipeline(CreateIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance());
  }

  @Test
  public void testMemWritable() throws Exception {
    run(MemPipeline.getInstance(), WritableTypeFamily.getInstance());
  }

  @Test
  public void testMemAvro() throws Exception {
    run(MemPipeline.getInstance(), AvroTypeFamily.getInstance());
  }

  @Test
  public void testSparkWritable() throws Exception {
    run(new SparkPipeline("local", "CreateIT", CreateIT.class, tmpDir.getDefaultConfiguration()),
            WritableTypeFamily.getInstance());
  }

  @Test
  public void testSparkAvro() throws Exception {
    run(new SparkPipeline("local", "CreateIT", CreateIT.class, tmpDir.getDefaultConfiguration()),
            AvroTypeFamily.getInstance());
  }

  public static void run(Pipeline p, PTypeFamily ptf) {
    PTable<String, Long> in = p.create(
            ImmutableList.of(
                    Pair.of("a", 2L), Pair.of("b", 3L), Pair.of("c", 5L),
                    Pair.of("a", 1L), Pair.of("b", 8L), Pair.of("c", 7L)),
            ptf.tableOf(ptf.strings(), ptf.longs()),
            CreateOptions.nameAndParallelism("in", 2));
    PTable<String, Long> out = in.groupByKey().combineValues(Aggregators.SUM_LONGS());
    Map<String, Long> values = out.materializeToMap();
    assertEquals(3, values.size());
    assertEquals(3L, values.get("a").longValue());
    assertEquals(11L, values.get("b").longValue());
    assertEquals(12L, values.get("c").longValue());
    p.done();
  }
}
