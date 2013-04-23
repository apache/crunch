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
 * A {@code PObject} represents a singleton object value that results from a distributed
 * computation. Computation producing the value is deferred until
 * {@link org.apache.crunch.PObject#getValue()} is called.
 *
 * @param <T> The type of value encapsulated by this {@code PObject}.
 */
public interface PObject<T> {
  /**
   * Gets the value associated with this {@code PObject}.  Calling this method will trigger
   * whatever computation is necessary to obtain the value and block until that computation
   * succeeds.
   *
   * @return The value associated with this {@code PObject}.
   */
  T getValue();
}
