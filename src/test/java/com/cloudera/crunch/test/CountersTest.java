/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.test;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Test;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;

/**
 * A test to verify using counters inside of a unit test works. :)
 */
public class CountersTest {

  public enum CT { ONE, TWO, THREE };
  
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
  
  @Test public void test() {
    CTFn fn = new CTFn();
    fn.process("foo", null);
    fn.process("bar", null);
    assertEquals(2L, TestCounters.getCounter(CT.ONE).getValue());
    assertEquals(8L, TestCounters.getCounter(CT.TWO).getValue());
    assertEquals(14L, TestCounters.getCounter(CT.THREE).getValue());
  }
  
  @Test public void secondTest() {
    CTFn fn = new CTFn();
    fn.process("foo", null);
    fn.process("bar", null);
    assertEquals(2L, TestCounters.getCounter(CT.ONE).getValue());
    assertEquals(8L, TestCounters.getCounter(CT.TWO).getValue());
    assertEquals(14L, TestCounters.getCounter(CT.THREE).getValue());
  }
}
