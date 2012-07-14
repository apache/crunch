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
package org.apache.crunch.fn;

import static org.junit.Assert.assertTrue;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.junit.Test;

@SuppressWarnings("serial")
public class PairMapTest {

  static final MapFn<String, Integer> one = new MapFn<String, Integer>() {
    @Override
    public Integer map(String input) {
      return 1;
    }
  };

  static final MapFn<String, Integer> two = new MapFn<String, Integer>() {
    @Override
    public Integer map(String input) {
      return 2;
    }
  };

  @Test
  public void testPairMap() {
    StoreLastEmitter<Pair<Integer, Integer>> emitter = StoreLastEmitter.create();
    PairMapFn<String, String, Integer, Integer> fn = new PairMapFn<String, String, Integer, Integer>(one, two);
    fn.process(Pair.of("a", "b"), emitter);
    Pair<Integer, Integer> pair = emitter.getLast();
    assertTrue(pair.first() == 1);
    assertTrue(pair.second() == 2);
  }
}
