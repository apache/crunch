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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

@SuppressWarnings("serial")
public class FilterFnTest {

  private static final FilterFn<String> TRUE = new FilterFn<String>() {
    @Override
    public boolean accept(String input) {
      return true;
    }
  };

  private static final FilterFn<String> FALSE = new FilterFn<String>() {
    @Override
    public boolean accept(String input) {
      return false;
    }
  };
  
  @Test
  public void testAnd() {
    assertTrue(FilterFn.and(TRUE).accept("foo"));
    assertTrue(FilterFn.and(TRUE, TRUE).accept("foo"));
    assertFalse(FilterFn.and(TRUE, FALSE).accept("foo"));
    assertFalse(FilterFn.and(FALSE, FALSE, FALSE).accept("foo"));
  }
  
  @Test
  public void testOr() {
    assertFalse(FilterFn.or(FALSE).accept("foo"));
    assertTrue(FilterFn.or(FALSE, TRUE).accept("foo"));
    assertTrue(FilterFn.or(TRUE, FALSE, TRUE).accept("foo"));
    assertFalse(FilterFn.or(FALSE, FALSE, FALSE).accept("foo"));
  }

  @Test
  public void testNot() {
    assertFalse(FilterFn.not(TRUE).accept("foo"));
    assertTrue(FilterFn.not(FALSE).accept("foo"));
  }
}
