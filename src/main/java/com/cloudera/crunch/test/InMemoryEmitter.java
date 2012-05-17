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

import java.util.List;

import com.cloudera.crunch.Emitter;
import com.google.common.collect.Lists;

/**
 * An {@code Emitter} instance that writes emitted records to a backing {@code List}.
 *
 * @param <T>
 */
public class InMemoryEmitter<T> implements Emitter<T> {
  
  private final List<T> output;
  
  public InMemoryEmitter() {
    this(Lists.<T>newArrayList());
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

  }

  public List<T> getOutput() {
    return output;
  }
}
