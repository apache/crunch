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
 * A convenience class for four-element {@link Tuple}s.
 */
public class Tuple4<V1, V2, V3, V4> implements Tuple {

  private final V1 first;
  private final V2 second;
  private final V3 third;
  private final V4 fourth;

  public static <A, B, C, D> Tuple4<A, B, C, D> of(A a, B b, C c, D d) {
    return new Tuple4<A, B, C, D>(a, b, c, d);
  }
  
  public Tuple4(V1 first, V2 second, V3 third, V4 fourth) {
    this.first = first;
    this.second = second;
    this.third = third;
    this.fourth = fourth;
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

  public V4 fourth() {
    return fourth;
  }

  public Object get(int index) {
    switch (index) {
    case 0:
      return first;
    case 1:
      return second;
    case 2:
      return third;
    case 3:
      return fourth;
    default:
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  public int size() {
    return 4;
  }
  
  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(first).append(second).append(third)
    	.append(fourth).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tuple4 other = (Tuple4) obj;
    return (first == other.first || (first != null && first.equals(other.first))) &&
    	(second == other.second || (second != null && second.equals(other.second))) &&
    	(third == other.third || (third != null && third.equals(other.third))) &&
    	(fourth == other.fourth || (fourth != null && fourth.equals(other.fourth)));
  }

  @Override
  public String toString() {
	StringBuilder sb = new StringBuilder("Tuple4[");
	sb.append(first).append(",").append(second).append(",").append(third);
	return sb.append(",").append(fourth).append("]").toString();
  }
}
