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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.cloudera.crunch.types.TupleFactory;

public class TupleTest {
  private String first = "foo";
  private Integer second = 1729;
  private Double third = 64.2;
  private Boolean fourth = false;
  private Float fifth = 17.29f;
  
  @Test
  public void testTuple3() {
    Tuple3<String, Integer, Double> t = new Tuple3<String, Integer, Double>(first, second, third);
    assertEquals(3, t.size());
    assertEquals(first, t.first());
    assertEquals(second, t.second());
    assertEquals(third, t.third());
    assertEquals(first, t.get(0));
    assertEquals(second, t.get(1));
    assertEquals(third, t.get(2));
    try {
      t.get(-1);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
  
  @Test
  public void testTuple3Equality() {
    Tuple3<String, Integer, Double> t = new Tuple3<String, Integer, Double>(first, second, third);
    assertTrue(t.equals(new Tuple3(first, second, third)));
    assertFalse(t.equals(new Tuple3(first, null, third)));
    assertFalse((new Tuple3(null, null, null)).equals(t));
    assertTrue((new Tuple3(first, null, null)).equals(new Tuple3(first, null, null)));
  }
  
  @Test
  public void testTuple4() {
    Tuple4<String, Integer, Double, Boolean> t = 
      new Tuple4<String, Integer, Double, Boolean>(first, second, third, fourth);
    assertEquals(4, t.size());
    assertEquals(first, t.first());
    assertEquals(second, t.second());
    assertEquals(third, t.third());
    assertEquals(fourth, t.fourth());
    assertEquals(first, t.get(0));
    assertEquals(second, t.get(1));
    assertEquals(third, t.get(2));
    assertEquals(fourth, t.get(3));
    try {
      t.get(-1);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }

  @Test
  public void testTuple4Equality() {
    Tuple4<String, Integer, Double, Boolean> t = 
      new Tuple4<String, Integer, Double, Boolean>(first, second, third, fourth);
    assertFalse(t.equals(new Tuple3(first, second, third)));
    assertFalse(t.equals(new Tuple4(first, null, third, null)));
    assertFalse((new Tuple4(null, null, null, null)).equals(t));
    assertTrue((new Tuple4(first, null, third, null)).equals(
        new Tuple4(first, null, third, null)));
  }

  @Test
  public void testTupleN() {
    TupleN t = new TupleN(first, second, third, fourth, fifth);
    assertEquals(5, t.size());
    assertEquals(first, t.get(0));
    assertEquals(second, t.get(1));
    assertEquals(third, t.get(2));
    assertEquals(fourth, t.get(3));
    assertEquals(fifth, t.get(4));
    try {
      t.get(-1);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }

  @Test
  public void testTupleNEquality() {
	TupleN t = new TupleN(first, second, third, fourth, fifth);
	assertTrue(t.equals(new TupleN(first, second, third, fourth, fifth)));
    assertFalse(t.equals(new TupleN(first, null, third, null)));
    assertFalse((new TupleN(null, null, null, null, null)).equals(t));
    assertTrue((new TupleN(first, second, third, null, null)).equals(
        new TupleN(first, second, third, null, null)));
  }

  @Test
  public void testTupleFactory() {
    checkTuple(TupleFactory.PAIR.makeTuple("a", "b"), Pair.class, "a", "b");
    checkTuple(TupleFactory.TUPLE3.makeTuple("a", "b", "c"), Tuple3.class, "a", "b", "c");
    checkTuple(TupleFactory.TUPLE4.makeTuple("a", "b", "c", "d"), Tuple4.class, "a", "b", "c", "d");
    checkTuple(TupleFactory.TUPLEN.makeTuple("a", "b", "c", "d", "e"), TupleN.class, "a", "b", "c", "d", "e");

    checkTuple(TupleFactory.TUPLEN.makeTuple("a", "b"), TupleN.class, "a", "b");
    checkTuple(TupleFactory.TUPLEN.makeTuple("a", "b", "c"), TupleN.class, "a", "b", "c");
    checkTuple(TupleFactory.TUPLEN.makeTuple("a", "b", "c", "d"), TupleN.class, "a", "b", "c", "d");
    checkTuple(TupleFactory.TUPLEN.makeTuple("a", "b", "c", "d", "e"), TupleN.class, "a", "b", "c", "d", "e");
  }

  private void checkTuple(Tuple t, Class<? extends Tuple> type, Object... values) {
    assertEquals(type, t.getClass());
    assertEquals(values.length, t.size());
    for (int i = 0; i < values.length; i++)
      assertEquals(values[i], t.get(i));
  }

}
