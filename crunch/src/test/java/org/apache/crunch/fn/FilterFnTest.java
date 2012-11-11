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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.crunch.FilterFn;
import org.junit.Test;

import com.google.common.base.Predicates;


public class FilterFnTest {

  private static final FilterFn<String> TRUE = FilterFns.<String>ACCEPT_ALL();
  private static final FilterFn<String> FALSE = FilterFns.<String>REJECT_ALL();

  @Test
  public void testAcceptAll() {
    assertThat(TRUE.accept(""), is(true));
    assertThat(TRUE.accept("foo"), is(true));
  }

  @Test
  public void testRejectAll() {
    assertThat(FALSE.accept(""), is(false));
    assertThat(FALSE.accept("foo"), is(false));

    Predicates.or(Predicates.alwaysFalse(), Predicates.alwaysTrue());
  }

  @Test
  public void testAnd() {
    assertThat(FilterFns.and(TRUE, TRUE).accept("foo"), is(true));
    assertThat(FilterFns.and(TRUE, FALSE).accept("foo"), is(false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGeneric() {
    assertThat(FilterFns.and(TRUE).accept("foo"), is(true));
    assertThat(FilterFns.and(FALSE).accept("foo"), is(false));
    assertThat(FilterFns.and(FALSE, FALSE, FALSE).accept("foo"), is(false));
    assertThat(FilterFns.and(TRUE, TRUE, FALSE).accept("foo"), is(false));
    assertThat(FilterFns.and(FALSE, FALSE, FALSE, FALSE).accept("foo"), is(false));
  }

  @Test
  public void testOr() {
    assertThat(FilterFns.or(FALSE, TRUE).accept("foo"), is(true));
    assertThat(FilterFns.or(TRUE, FALSE).accept("foo"), is(true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOrGeneric() {
    assertThat(FilterFns.or(TRUE).accept("foo"), is(true));
    assertThat(FilterFns.or(FALSE).accept("foo"), is(false));
    assertThat(FilterFns.or(TRUE, FALSE, TRUE).accept("foo"), is(true));
    assertThat(FilterFns.or(FALSE, FALSE, TRUE).accept("foo"), is(true));
    assertThat(FilterFns.or(FALSE, FALSE, FALSE).accept("foo"), is(false));
  }

  @Test
  public void testNot() {
    assertThat(FilterFns.not(TRUE).accept("foo"), is(false));
    assertThat(FilterFns.not(FALSE).accept("foo"), is(true));
  }
}
