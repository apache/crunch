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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.crunch.FilterFn.AndFn;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Before;
import org.junit.Test;

public class AndFnTest {

  private FilterFn<Integer> fnA;
  private FilterFn<Integer> fnB;
  private AndFn<Integer> andFn;

  @Before
  public void setUp() {
    fnA = mock(FilterFn.class);
    fnB = mock(FilterFn.class);
    andFn = new AndFn(fnA, fnB);
  }

  @Test
  public void testSetContext() {
    TaskInputOutputContext<?, ?, ?, ?> context = mock(TaskInputOutputContext.class);
    andFn.setContext(context);

    verify(fnA).setContext(context);
    verify(fnB).setContext(context);
  }

  @Test
  public void testAccept_False() {
    when(fnA.accept(1)).thenReturn(true);
    when(fnB.accept(1)).thenReturn(false);

    assertFalse(andFn.accept(1));
  }

  @Test
  public void testAccept_True() {
    when(fnA.accept(1)).thenReturn(true);
    when(fnB.accept(1)).thenReturn(true);

    assertTrue(andFn.accept(1));
  }

  @Test
  public void testCleanup() {
    andFn.cleanup(mock(Emitter.class));

    verify(fnA).cleanup();
    verify(fnB).cleanup();
  }

}
