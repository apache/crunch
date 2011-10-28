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

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A convenience class for three-element {@link Tuple}s.
 */
public class Tuple3<V1, V2, V3> implements Tuple {

  private final V1 first;
  private final V2 second;
  private final V3 third;

  public static <A, B, C> Tuple3<A, B, C> of(A a, B b, C c) {
    return new Tuple3<A, B, C>(a, b, c);
  }
  
  public Tuple3(V1 first, V2 second, V3 third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  public V1 first() {
    return first;
  }

  public V2 second() {
    return second;
  }

  public V3 third() {
    return third;
  }

  public Object get(int index) {
    switch (index) {
    case 0:
      return first;
    case 1:
      return second;
    case 2:
      return third;
    default:
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  public int size() {
    return 3;
  }
  
  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(first).append(second).append(third).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tuple3 other = (Tuple3) obj;
    return (first == other.first || (first != null && first.equals(other.first))) &&
    	(second == other.second || (second != null && second.equals(other.second))) &&
    	(third == other.third || (third != null && third.equals(other.third)));
  }

  @Override
  public String toString() {
	StringBuilder sb = new StringBuilder("Tuple3[");
	sb.append(first).append(",").append(second).append(",").append(third);
	return sb.append("]").toString();
  }
}
