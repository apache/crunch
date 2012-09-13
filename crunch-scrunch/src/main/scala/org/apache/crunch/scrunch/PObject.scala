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
package org.apache.crunch.scrunch

import org.apache.crunch.{PObject => JPObject}
import org.apache.crunch.Target

/**
 * Represents a singleton value that results from a distributed computation.
 *
 * @param native The Java PObject that backs this Scala PObject.
 * @tparam T The type of value encapsulated by this PObject.
 */
class PObject[T] private (private val native: JPObject[T]) {
  /**
   * Gets the value associated with this PObject.  Calling this method will trigger
   * whatever computation is necessary to obtain the value and block until that computation
   * succeeds.
   *
   * @return The value associated with this PObject.
   */
  def value(): T = native.getValue()
}

/**
 * The companion object for PObject that provides factory methods for creating PObjects.
 */
protected[scrunch] object PObject {

  /**
   * Creates a new Scala PObject from a Java PObject.
   *
   * @param native The Java PObject that will back this Scala PObject.
   * @tparam T The type of value encapsulated by the PObject.
   * @return A Scala PObject backed by the Java PObject specified.
   */
  def apply[T](native: JPObject[T]): PObject[T] = new PObject[T](native)
}
