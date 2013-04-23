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

import static org.junit.Assert.assertEquals;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.junit.Test;

@SuppressWarnings("serial")
public class ExtractKeyFnTest {

  protected static final MapFn<String, Integer> mapFn = new MapFn<String, Integer>() {
    @Override
    public Integer map(String input) {
      return input.hashCode();
    }
  };

  protected static final ExtractKeyFn<Integer, String> one = new ExtractKeyFn<Integer, String>(mapFn);

  @Test
  public void test() {
    StoreLastEmitter<Pair<Integer, String>> emitter = StoreLastEmitter.create();
    one.process("boza", emitter);
    assertEquals(Pair.of("boza".hashCode(), "boza"), emitter.getLast());
  }
}
