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
package org.apache.crunch.io.avro;

import static org.apache.crunch.types.avro.Avros.ints;
import static org.apache.crunch.types.avro.Avros.tableOf;
import static org.apache.crunch.types.avro.Avros.writables;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Map;

import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Verify handling of both a ByteBuffer and byte array as input from an Avro job (depending
 * on the version of Avro being used).
 */
public class AvroWritableIT implements Serializable {

  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();
  
  @Test
  public void testAvroBasedWritablePipeline() throws Exception {
    String customersInputPath = tmpDir.copyResourceFileName("customers.txt");
    Pipeline pipeline = new MRPipeline(AvroWritableIT.class, tmpDir.getDefaultConfiguration());
    pipeline.enableDebug();
    PCollection<String> customerLines = pipeline.readTextFile(customersInputPath);
    Map<Integer, DoubleWritable> outputMap = customerLines.parallelDo(
        new MapFn<String, Pair<Integer, DoubleWritable>>() {
          @Override
          public Pair<Integer, DoubleWritable> map(String input) {
            int len = input.length();
            return Pair.of(len, new DoubleWritable(len));
          }
        }, tableOf(ints(), writables(DoubleWritable.class)))
    .groupByKey()
    .combineValues(new CombineFn<Integer, DoubleWritable>() {
      @Override
      public void process(Pair<Integer, Iterable<DoubleWritable>> input,
          Emitter<Pair<Integer, DoubleWritable>> emitter) {
        double sum = 0.0;
        for (DoubleWritable dw : input.second()) {
          sum += dw.get();
        }
        emitter.emit(Pair.of(input.first(), new DoubleWritable(sum)));
      }
    })
    .materializeToMap();
    
    Map<Integer, DoubleWritable> expectedMap = Maps.newHashMap();
    expectedMap.put(17, new DoubleWritable(17.0));
    expectedMap.put(16, new DoubleWritable(16.0));
    expectedMap.put(12, new DoubleWritable(24.0));
   
    assertEquals(expectedMap, outputMap);
    
    pipeline.done();
  }
}
