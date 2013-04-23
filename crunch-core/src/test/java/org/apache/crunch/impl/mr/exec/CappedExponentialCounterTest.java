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
package org.apache.crunch.impl.mr.exec;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CappedExponentialCounterTest {

  @Test
  public void testGet() {
    CappedExponentialCounter c = new CappedExponentialCounter(1L, Long.MAX_VALUE);
    assertEquals(1L, c.get());
    assertEquals(2L, c.get());
    assertEquals(4L, c.get());
    assertEquals(8L, c.get());
  }

  @Test
  public void testCap() {
    CappedExponentialCounter c = new CappedExponentialCounter(1L, 2);
    assertEquals(1L, c.get());
    assertEquals(2L, c.get());
    assertEquals(2L, c.get());
  }
}
