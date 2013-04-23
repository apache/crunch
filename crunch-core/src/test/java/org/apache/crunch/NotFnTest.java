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

import static org.junit.Assert.*;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.crunch.FilterFn.NotFn;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Before;
import org.junit.Test;

public class NotFnTest {
  
  private FilterFn<Integer> base;
  private NotFn<Integer> notFn;
  
  @Before
  public void setUp() {
    base = mock(FilterFn.class);
    notFn = new NotFn(base);
  }

  @Test
  public void testSetContext() {
    TaskInputOutputContext<?, ?, ?, ?> context = mock(TaskInputOutputContext.class);
    
    notFn.setContext(context);
    
    verify(base).setContext(context);
  }

  @Test
  public void testAccept_True() {
    when(base.accept(1)).thenReturn(true);
    
    assertFalse(notFn.accept(1));
  }
  
  @Test
  public void testAccept_False() {
    when(base.accept(1)).thenReturn(false);
    
    assertTrue(notFn.accept(1));
  }

  @Test
  public void testCleanupEmitterOfT() {
    notFn.cleanup(mock(Emitter.class));
    
    verify(base).cleanup();
  }

}
