/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.crunch;

/**
 * A convenience class for two-element {@link Tuple}s.
 */
public class Pair<K, V> extends Tuple {

  private final K first;
  private final V second;

  public static <T, U> Pair<T, U> of(T first, U second) {
    return new Pair<T, U>(first, second);
  }

  public Pair(K first, V second) {
    this.first = first;
    this.second = second;
  }

  public K first() {
    return first;
  }

  public V second() {
    return second;
  }

  public Object get(int index) {
    switch (index) {
    case 0:
      return first;
    case 1:
      return second;
    default:
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  public int size() {
    return 2;
  }
}
