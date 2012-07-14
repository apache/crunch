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
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class CartesianTest {

  @Test
  public void testCartesianCollection() {
    ImmutableList<ImmutableList<Integer>> testCases = ImmutableList.of(ImmutableList.of(1, 2, 3, 4, 5),
        ImmutableList.<Integer> of(1, 2, 3), ImmutableList.<Integer> of());

    for (int t1 = 0; t1 < testCases.size(); t1++) {
      ImmutableList<Integer> testCase1 = testCases.get(t1);
      for (int t2 = t1; t2 < testCases.size(); t2++) {
        ImmutableList<Integer> testCase2 = testCases.get(t2);

        PCollection<Integer> X = MemPipeline.typedCollectionOf(Writables.ints(), testCase1);
        PCollection<Integer> Y = MemPipeline.typedCollectionOf(Writables.ints(), testCase2);

        PCollection<Pair<Integer, Integer>> cross = Cartesian.cross(X, Y);
        HashSet<Pair<Integer, Integer>> crossSet = new HashSet<Pair<Integer, Integer>>();
        for (Iterator<Pair<Integer, Integer>> i = cross.materialize().iterator(); i.hasNext();) {
          crossSet.add(i.next());
        }
        assertEquals(crossSet.size(), testCase1.size() * testCase2.size());

        for (int i = 0; i < testCase1.size(); i++) {
          for (int j = 0; j < testCase2.size(); j++) {
            assertTrue(crossSet.contains(Pair.of(testCase1.get(i), testCase2.get(j))));
          }
        }
      }
    }
  }

}
