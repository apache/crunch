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

import java.io.Serializable;
import java.util.Map;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class MapsIT {
  @Rule
  public TemporaryPath tmpDir = new TemporaryPath();

  @Test
  public void testWritables() throws Exception {
    run(WritableTypeFamily.getInstance(), tmpDir);
  }

  @Test
  public void testAvros() throws Exception {
    run(AvroTypeFamily.getInstance(), tmpDir);
  }

  public static void run(PTypeFamily typeFamily, TemporaryPath tmpDir) throws Exception {
    Pipeline pipeline = new MRPipeline(MapsIT.class, tmpDir.setTempLoc(new Configuration()));
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");
    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    Iterable<Pair<String, Map<String, Long>>> output = shakespeare
        .parallelDo(new DoFn<String, Pair<String, Map<String, Long>>>() {
          @Override
          public void process(String input, Emitter<Pair<String, Map<String, Long>>> emitter) {
            String last = null;
            for (String word : input.toLowerCase().split("\\W+")) {
              if (!word.isEmpty()) {
                String firstChar = word.substring(0, 1);
                if (last != null) {
                  Map<String, Long> cc = ImmutableMap.of(firstChar, 1L);
                  emitter.emit(Pair.of(last, cc));
                }
                last = firstChar;
              }
            }
          }
        }, typeFamily.tableOf(typeFamily.strings(), typeFamily.maps(typeFamily.longs()))).groupByKey()
        .combineValues(new CombineFn<String, Map<String, Long>>() {
          @Override
          public void process(Pair<String, Iterable<Map<String, Long>>> input,
              Emitter<Pair<String, Map<String, Long>>> emitter) {
            Map<String, Long> agg = Maps.newHashMap();
            for (Map<String, Long> in : input.second()) {
              for (Map.Entry<String, Long> e : in.entrySet()) {
                if (!agg.containsKey(e.getKey())) {
                  agg.put(e.getKey(), e.getValue());
                } else {
                  agg.put(e.getKey(), e.getValue() + agg.get(e.getKey()));
                }
              }
            }
            emitter.emit(Pair.of(input.first(), agg));
          }
        }).materialize();
    boolean passed = false;
    for (Pair<String, Map<String, Long>> v : output) {
      if (v.first() == "k" && v.second().get("n") == 8L) {
        passed = true;
        break;
      }
    }
    pipeline.done();
  }
}
