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

import java.util.Collection;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ChannelsTest {

  /**
   * Test that non-null values in a PCollection<Pair<>> are split properly into
   * a Pair<PCollection<>,PCollection<>>
   */
  @Test
  public void split() {
    // Test that any combination of values and nulls are handled properly
    final PCollection<Pair<String, String>> pCollection = MemPipeline.typedCollectionOf(
        Writables.pairs(Writables.strings(), Writables.strings()),
        ImmutableList.of(Pair.of("One", (String) null), Pair.of((String) null, "Two"), Pair.of("Three", "Four"),
            Pair.of((String) null, (String) null)));

    final Pair<PCollection<String>, PCollection<String>> splitPCollection = Channels.split(pCollection);

    final Collection<String> firstCollection = splitPCollection.first().asCollection().getValue();
    assertEquals(2, firstCollection.size());
    assertTrue(firstCollection.contains("One"));
    assertTrue(firstCollection.contains("Three"));

    final Collection<String> secondCollection = splitPCollection.second().asCollection().getValue();
    assertEquals(2, secondCollection.size());
    assertTrue(secondCollection.contains("Two"));
    assertTrue(secondCollection.contains("Four"));
  }
}
