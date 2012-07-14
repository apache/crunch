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
package org.apache.crunch.test;

import static org.junit.Assert.assertEquals;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.junit.After;
import org.junit.Test;

/**
 * A test to verify using counters inside of a unit test works. :)
 */
public class CountersTest {

  public enum CT {
    ONE,
    TWO,
    THREE
  };

  @After
  public void after() {
    TestCounters.clearCounters();
  }

  public static class CTFn extends DoFn<String, String> {
    @Override
    public void process(String input, Emitter<String> emitter) {
      getCounter(CT.ONE).increment(1);
      getCounter(CT.TWO).increment(4);
      getCounter(CT.THREE).increment(7);
    }
  }

  @Test
  public void test() {
    CTFn fn = new CTFn();
    fn.process("foo", null);
    fn.process("bar", null);
    assertEquals(2L, TestCounters.getCounter(CT.ONE).getValue());
    assertEquals(8L, TestCounters.getCounter(CT.TWO).getValue());
    assertEquals(14L, TestCounters.getCounter(CT.THREE).getValue());
  }

  @Test
  public void secondTest() {
    CTFn fn = new CTFn();
    fn.process("foo", null);
    fn.process("bar", null);
    assertEquals(2L, TestCounters.getCounter(CT.ONE).getValue());
    assertEquals(8L, TestCounters.getCounter(CT.TWO).getValue());
    assertEquals(14L, TestCounters.getCounter(CT.THREE).getValue());
  }
}
