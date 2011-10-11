/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.fn;

import static org.junit.Assert.assertEquals;
import com.cloudera.crunch.Pair;
import org.junit.Test;

@SuppressWarnings("serial")
public class MapValuesTest {
  
  static final MapValuesFn<String, String, Integer> one = new MapValuesFn<String, String, Integer>() {
    @Override
    public Integer map(String input) {
      return 1;
    }
  };
  
  static final MapValuesFn<String, String, Integer> two = new MapValuesFn<String, String, Integer>() {
    @Override
    public Integer map(String input) {
      return 2;
    }
  };
  
  @Test
  public void test() {
    StoreLastEmitter<Pair<String, Integer>> emitter = StoreLastEmitter.create();
    one.process(Pair.of("k", "v"), emitter);
    assertEquals(Pair.of("k", 1), emitter.getLast());
    two.process(Pair.of("k", "v"), emitter);
    assertEquals(Pair.of("k", 2), emitter.getLast());
  }
}
