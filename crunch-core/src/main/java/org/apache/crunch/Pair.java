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

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * A convenience class for two-element {@link Tuple}s.
 */
public class Pair<K, V> implements Tuple, Comparable<Pair<K, V>>, Serializable {

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

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(first).append(second).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Pair<?, ?> other = (Pair<?, ?>) obj;
    return (first == other.first || (first != null && first.equals(other.first)))
        && (second == other.second || (second != null && second.equals(other.second)));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    sb.append(first).append(",").append(second).append("]");
    return sb.toString();
  }

  private int cmp(Object lhs, Object rhs) {
    if (lhs == rhs) {
      return 0;
    } else if (lhs != null && Comparable.class.isAssignableFrom(lhs.getClass())) {
      return ((Comparable) lhs).compareTo(rhs);
    }
    return (lhs == null ? 0 : lhs.hashCode()) - (rhs == null ? 0 : rhs.hashCode());
  }

  @Override
  public int compareTo(Pair<K, V> o) {
    int diff = cmp(first, o.first);
    if (diff == 0) {
      diff = cmp(second, o.second);
    }
    return diff;
  }
}
