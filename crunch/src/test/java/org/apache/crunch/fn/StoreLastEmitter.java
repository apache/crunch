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

import org.apache.crunch.Emitter;

class StoreLastEmitter<T> implements Emitter<T> {
  private T last;

  @Override
  public void emit(T emitted) {
    last = emitted;
  }

  public T getLast() {
    return last;
  }

  @Override
  public void flush() {
  }

  public static <T> StoreLastEmitter<T> create() {
    return new StoreLastEmitter<T>();
  }
}