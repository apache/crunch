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

import java.util.Collection;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

/**
 * A convenience class for four-element {@link Tuple}s.
 */
public class Tuple4<V1, V2, V3, V4> implements Tuple {

  public static class Collect<V1, V2, V3, V4> extends Tuple4<
  Collection<V1>, 
  Collection<V2>,
  Collection<V3>,
  Collection<V4>> {

    public static <V1, V2, V3, V4> PType<Tuple4.Collect<V1, V2, V3, V4>> derived(PType<V1> first,
        PType<V2> second, PType<V3> third, PType<V4> fourth) {
      PTypeFamily tf = first.getFamily();
      PType<Tuple4<Collection<V1>, Collection<V2>, Collection<V3>, Collection<V4>>> pt = 
          tf.quads(
              tf.collections(first),
              tf.collections(second),
              tf.collections(third),
              tf.collections(fourth));
      Object clazz = Tuple4.Collect.class;
      return tf.derived((Class<Tuple4.Collect<V1, V2, V3, V4>>) clazz,
          new MapFn<Tuple4<Collection<V1>, Collection<V2>, Collection<V3>, Collection<V4>>,
          Collect<V1, V2, V3, V4>>() {
        @Override
        public Collect<V1, V2, V3, V4> map(
            Tuple4<Collection<V1>, Collection<V2>, Collection<V3>, Collection<V4>> in) {
          return new Collect<V1, V2, V3, V4>(in.first(), in.second(), in.third(), in.fourth());
        }
      },
      new MapFn<Collect<V1, V2, V3, V4>, Tuple4<Collection<V1>, Collection<V2>, Collection<V3>, Collection<V4>>>() {
        @Override
        public Tuple4<Collection<V1>, Collection<V2>, Collection<V3>, Collection<V4>> map(
            Collect<V1, V2, V3, V4> input) {
          return input;
        }
      }, pt);
    }

    public Collect(Collection<V1> first, Collection<V2> second, Collection<V3> third, Collection<V4> fourth) {
      super(first, second, third, fourth);
    }
  }
  
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
    return hcb.append(first).append(second).append(third).append(fourth).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tuple4<?, ?, ?, ?> other = (Tuple4<?, ?, ?, ?>) obj;
    return (first == other.first || (first != null && first.equals(other.first)))
        && (second == other.second || (second != null && second.equals(other.second)))
        && (third == other.third || (third != null && third.equals(other.third)))
        && (fourth == other.fourth || (fourth != null && fourth.equals(other.fourth)));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Tuple4[");
    sb.append(first).append(",").append(second).append(",").append(third);
    return sb.append(",").append(fourth).append("]").toString();
  }
}
