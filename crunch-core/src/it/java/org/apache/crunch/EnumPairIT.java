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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypes;
import org.apache.crunch.types.writable.Writables;
import org.junit.Rule;
import org.junit.Test;

public class EnumPairIT implements Serializable {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  enum etypes {
    type1,
  }

  @Test
  public void testEnumPTypes() throws IOException {
    String inputFile1 = tmpDir.copyResourceFileName("set1.txt");
    Pipeline pipeline = new MRPipeline(EnumPairIT.class);
    PCollection<String> set1 = pipeline.readTextFile(inputFile1);
    PTable<String, etypes> data = set1.parallelDo(new DoFn<String, Pair<String, etypes>>() {
      @Override
      public void process(String input, Emitter<Pair<String, etypes>> emitter) {
        emitter.emit(new Pair<String, etypes>(input, etypes.type1));
      }
    }, Writables.tableOf(Writables.strings(), PTypes.enums(etypes.class, set1.getTypeFamily())));

    Iterable<Pair<String, etypes>> materialized = data.materialize();
    pipeline.run();
    for (Pair<String, etypes> pair : materialized) {
      assertEquals(etypes.type1, pair.second());
    }
  }
}
