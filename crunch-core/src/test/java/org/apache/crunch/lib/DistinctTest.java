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

import java.util.Arrays;
import java.util.List;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class DistinctTest {
  private static final List<Integer> DATA = Arrays.asList(
      17, 29, 17, 29, 17, 29, 36, 45, 17, 45, 36, 29
  );

  @Test
  public void testDistinct() {
    PCollection<Integer> input = MemPipeline.typedCollectionOf(Avros.ints(), DATA);
    Iterable<Integer> unique = Distinct.distinct(input).materialize();

    assertEquals(ImmutableSet.copyOf(DATA), ImmutableSet.copyOf(unique));
  }

  @Test
  public void testDistinctFlush() {
    PCollection<Integer> input = MemPipeline.typedCollectionOf(Avros.ints(), DATA);
    Iterable<Integer> unique = Distinct.distinct(input, 2).materialize();

    assertEquals(ImmutableSet.copyOf(DATA), ImmutableSet.copyOf(unique));
  }
}
