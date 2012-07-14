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

/**
 * A {@link DoFn} for the common case of emitting exactly one value for each
 * input record.
 * 
 */
public abstract class MapFn<S, T> extends DoFn<S, T> {

  /**
   * Maps the given input into an instance of the output type.
   */
  public abstract T map(S input);

  @Override
  public void process(S input, Emitter<T> emitter) {
    emitter.emit(map(input));
  }

  @Override
  public float scaleFactor() {
    return 1.0f;
  }
}
