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

import com.google.common.collect.ImmutableList;

public class CombineFnTest {
  @Test
  public void testSums() {
    assertEquals(new Long(1775L),
        CombineFn.SUM_LONGS().combine(ImmutableList.of(29L, 17L, 1729L)));
    
    assertEquals(new Integer(1775),
        CombineFn.SUM_INTS().combine(ImmutableList.of(29, 17, 1729)));

    assertEquals(new Float(1775.0f),
        CombineFn.SUM_FLOATS().combine(ImmutableList.of(29f, 17f, 1729f)));

    assertEquals(new Double(1775.0),
        CombineFn.SUM_DOUBLES().combine(ImmutableList.of(29.0, 17.0, 1729.0)));
  }
  
  @Test
  public void testMax() {
    assertEquals(new Long(1729L),
        CombineFn.MAX_LONGS().combine(ImmutableList.of(29L, 17L, 1729L)));
    
    assertEquals(new Integer(1729),
        CombineFn.MAX_INTS().combine(ImmutableList.of(29, 17, 1729)));

    assertEquals(new Float(1729.0f),
        CombineFn.MAX_FLOATS().combine(ImmutableList.of(29f, 17f, 1729f)));

    assertEquals(new Double(1729.0),
        CombineFn.MAX_DOUBLES().combine(ImmutableList.of(29.0, 17.0, 1729.0)));
  }
  
  @Test
  public void testMin() {
    assertEquals(new Long(17L),
        CombineFn.MIN_LONGS().combine(ImmutableList.of(29L, 17L, 1729L)));
    
    assertEquals(new Integer(17),
        CombineFn.MIN_INTS().combine(ImmutableList.of(29, 17, 1729)));

    assertEquals(new Float(17.0f),
        CombineFn.MIN_FLOATS().combine(ImmutableList.of(29f, 17f, 1729f)));

    assertEquals(new Double(17.0),
        CombineFn.MIN_DOUBLES().combine(ImmutableList.of(29.0, 17.0, 1729.0)));
  }
}
