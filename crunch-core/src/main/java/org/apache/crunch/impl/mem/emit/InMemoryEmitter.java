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
package org.apache.crunch.impl.mem.emit;

import java.util.List;

import org.apache.crunch.Emitter;

import com.google.common.collect.Lists;
import org.apache.crunch.Pair;

/**
 * An {@code Emitter} instance that writes emitted records to a backing
 * {@code List}.
 * 
 * @param <T>
 */
public class InMemoryEmitter<T> implements Emitter<T> {

  private final List<T> output;

  public static <T> InMemoryEmitter<T> create() {
    return new InMemoryEmitter<T>();
  }

  public InMemoryEmitter() {
    this(Lists.<T> newArrayList());
  }

  public InMemoryEmitter(List<T> output) {
    this.output = output;
  }

  @Override
  public void emit(T emitted) {
    output.add(emitted);
  }

  @Override
  public void flush() {
    output.clear();
  }

  public List<T> getOutput() {
    return output;
  }
}
