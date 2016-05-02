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

package org.apache.crunch.impl.mem;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

public class MemPipelinePCollectionReuseTest {

  /**
   * Specific test for the situation outlined in CRUNCH-607, which was that deriving two PCollections from the same
   * PGroupedTable would throw an IllegalStateException from SingleUseIterable. This just ensures that this case
   * doesn't return.
   */
  @Test
  public void testGroupedCollectionReuse() {

    PCollection<String> stringValues = MemPipeline.typedCollectionOf(Avros.strings(), "one", "two", "three");

    PGroupedTable<String, String> groupedTable =
        stringValues.by(IdentityFn.<String>getInstance(), Avros.strings()).groupByKey();

    // Here we re-use the grouped table twice, meaning its internal iterators will need to be iterated multiple times
    PTable<String, Integer> stringLengthTable =
        groupedTable.mapValues(new MaxStringLengthFn(), Avros.ints());

    // Previous to LP-607, this would fail with an IllegalStateException from SingleUseIterable
    Set<String> keys = ImmutableSet.copyOf(groupedTable.ungroup().join(stringLengthTable).keys().materialize());

    assertEquals(
        ImmutableSet.of("one", "two", "three"),
        keys);
  }


  public static class MaxStringLengthFn extends MapFn<Iterable<String>, Integer> {
    @Override
    public Integer map(Iterable<String> input) {
      int maxLength = Integer.MIN_VALUE;
      for (String inputString : input) {
        maxLength = Math.max(maxLength, inputString.length());
      }
      return maxLength;
    }
  }


}
