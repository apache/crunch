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

import java.util.Iterator;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.junit.Rule;
import org.junit.Test;

public class SingleUseIterableExceptionIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  static class ReduceFn extends MapFn<Iterable<String>, String> {
    @Override
    public String map(Iterable<String> input) {
      Iterator<String> iter = input.iterator();
      throw new CrunchRuntimeException("Exception");
    }
  }
  
  @Test
  public void testException() throws Exception {
    run(new MRPipeline(SingleUseIterableExceptionIT.class),
        tmpDir.copyResourceFileName("shakes.txt"),
        tmpDir.getFileName("out"));
  }
  
  public static void run(MRPipeline p, String input, String output) {
    PCollection<String> shakes = p.readTextFile(input);
    shakes.parallelDo(new MapFn<String, Pair<String, String>>() {
      @Override
      public Pair<String, String> map(String input) {
        if (input.length() > 5) {
          return Pair.of(input.substring(0, 5), input);
        } else {
          return Pair.of("__SHORT__", input);
        }
      } 
    }, Avros.tableOf(Avros.strings(), Avros.strings()))
    .groupByKey()
    .mapValues(new ReduceFn(), Avros.strings())
    .write(To.textFile(output));
    p.done();
  }
}
