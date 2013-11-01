/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package org.apache.crunch.impl.mem;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.collect.MemCollection;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import java.util.Arrays;

public class CountersTest {

  @Test
  public void counterTest() throws Exception {
    Pipeline pipeline = MemPipeline.getInstance();

    // Single row PCollection.
    PCollection<String> objects = MemPipeline.collectionOf(Arrays.asList(new String[]{"hello world"}));
    System.out.println("Objects: " + ((MemCollection) objects).getCollection());

    // Counter creating Map.
    PCollection<String> objects2 = objects.parallelDo("Create counters",
        new MapFn<String, String>() {
          @Override
          public String map(String input) {
            for(int i = 0; i < 200; ++i) {
              this.increment("testCounter", String.valueOf(i));
            }
            return input;
          }
        },
        Writables.strings()
    );

    // Run it!
    pipeline.done();
    System.out.println("Objects2: " + ((MemCollection) objects2).getCollection());
  }
}
