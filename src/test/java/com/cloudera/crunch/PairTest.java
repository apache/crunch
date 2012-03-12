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
package com.cloudera.crunch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class PairTest {
  
  @Test
  public void testPairConstructor() {
    Pair<String, Integer> pair = new Pair<String, Integer>("brock", 45);
    test(pair);
  }

  @Test
  public void testPairOf() {
    Pair<String, Integer> pair = Pair.of("brock", 45);
    test(pair);
  }

  protected void test(Pair<String, Integer> pair) {
    assertTrue(pair.size() == 2);
    
    assertEquals("brock", pair.first());
    assertEquals(new Integer(45), pair.second());
    assertEquals(Pair.of("brock", 45), pair);
    
    assertEquals("brock", pair.get(0));
    assertEquals(new Integer(45), pair.get(1));

    try {
      pair.get(-1);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
  
  @Test
  public void testPairComparisons() {
    assertEquals(0, Pair.of(null, null).compareTo(Pair.of(null, null)));
    assertEquals(0, Pair.of(1, 2).compareTo(Pair.of(1, 2)));
    assertTrue(Pair.of(2, "a").compareTo(Pair.of(1, "a")) > 0);
    assertTrue(Pair.of("a", 2).compareTo(Pair.of("a", 1)) > 0);
    assertTrue(Pair.of(null, 17).compareTo(Pair.of(null, 29)) < 0);
  }
}
