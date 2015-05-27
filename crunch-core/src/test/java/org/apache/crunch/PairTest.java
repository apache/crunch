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
    assertTrue(Pair.of(Integer.MIN_VALUE, 0).compareTo(Pair.of((Integer)null, 0)) < 0);
    assertTrue(Pair.of(0, Integer.MIN_VALUE).compareTo(Pair.of(0, (Integer)null)) < 0);
    assertTrue(Pair.of(2, "a").compareTo(Pair.of(1, "a")) > 0);
    assertTrue(Pair.of(new HashValue(2), "a").compareTo(Pair.of(new HashValue(1), "a")) > 0);
    assertTrue(Pair.of(new HashValue(Integer.MAX_VALUE), "a").compareTo(
          Pair.of(new HashValue(Integer.MIN_VALUE), "a")) > 0);
    assertTrue(Pair.of(new HashValue(0), "a").compareTo(Pair.of((HashValue) null, "a")) < 0);
    // Two value whose hash code is the same but are not equal (via equals) should not compare as the same
    assertTrue(Pair.of(new HashValue(0, 1), 0).compareTo(Pair.of(new HashValue(0, 2), 0)) != 0);
  }

  /**
   * An object with a known hash value that is not comparable used for testing the comparison logic
   * within Pair.
   */
  private static final class HashValue {
    private final int value;
    private final int additionalValue;  // Additional value which is used for equality but not hash code

    public HashValue(int value) {
      this(value, 0);
    }

    public HashValue(int value, int additionalValue) {
      this.value = value;
      this.additionalValue = additionalValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof HashValue))
        return false;

      HashValue other = (HashValue) o;

      if (value != other.value)
        return false;

      if (additionalValue != other.additionalValue)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return this.value;
    }
  }
}
