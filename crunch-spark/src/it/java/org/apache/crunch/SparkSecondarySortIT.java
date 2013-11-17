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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.test.CrunchTestSupport;
import org.junit.Test;

import java.io.Serializable;

import static org.apache.crunch.types.avro.Avros.*;
import static org.apache.crunch.types.avro.Avros.ints;
import static org.apache.crunch.types.avro.Avros.strings;
import static org.junit.Assert.assertEquals;

public class SparkSecondarySortIT extends CrunchTestSupport implements Serializable {
  @Test
  public void testSecondarySort() throws Exception {
    Pipeline p = new SparkPipeline("local", "secondarysort");
    String inputFile = tempDir.copyResourceFileName("secondary_sort_input.txt");

    PTable<String, Pair<Integer, Integer>> in = p.read(From.textFile(inputFile))
        .parallelDo(new MapFn<String, Pair<String, Pair<Integer, Integer>>>() {
          @Override
          public Pair<String, Pair<Integer, Integer>> map(String input) {
            String[] pieces = input.split(",");
            return Pair.of(pieces[0],
                Pair.of(Integer.valueOf(pieces[1].trim()), Integer.valueOf(pieces[2].trim())));
          }
        }, tableOf(strings(), pairs(ints(), ints())));
    Iterable<String> lines = SecondarySort.sortAndApply(in, new MapFn<Pair<String, Iterable<Pair<Integer, Integer>>>, String>() {
      @Override
      public String map(Pair<String, Iterable<Pair<Integer, Integer>>> input) {
        Joiner j = Joiner.on(',');
        return j.join(input.first(), j.join(input.second()));
      }
    }, strings()).materialize();
    assertEquals(ImmutableList.of("one,[-5,10],[1,1],[2,-3]", "three,[0,-1]", "two,[1,7],[2,6],[4,5]"),
        ImmutableList.copyOf(lines));
    p.done();
  }
}
