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

import org.junit.Test;

public class TupleTest {
  private String first = "foo";
  private Integer second = 1729;
  private Double third = 64.2;
  private Boolean fourth = false;
  private Float fifth = 17.29f;
  
  @Test
  public void testTuple3() {
    Tuple3 t = new Tuple3(first, second, third);
    assertEquals(3, t.size());
    assertEquals(first, t.first());
    assertEquals(second, t.second());
    assertEquals(third, t.third());
    assertEquals(first, t.get(0));
    assertEquals(second, t.get(1));
    assertEquals(third, t.get(2));    
  }
  
  @Test
  public void testTuple4() {
    Tuple4 t = new Tuple4(first, second, third, fourth);
    assertEquals(4, t.size());
    assertEquals(first, t.first());
    assertEquals(second, t.second());
    assertEquals(third, t.third());
    assertEquals(fourth, t.fourth());
    assertEquals(first, t.get(0));
    assertEquals(second, t.get(1));
    assertEquals(third, t.get(2));
    assertEquals(fourth, t.get(3));
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
  }

}
