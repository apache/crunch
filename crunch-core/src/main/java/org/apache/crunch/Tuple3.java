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
 * A convenience class for three-element {@link Tuple}s.
 */
public class Tuple3<V1, V2, V3> implements Tuple {

  public static class Collect<V1, V2, V3> extends Tuple3<Collection<V1>, Collection<V2>, Collection<V3>> {

    public static <V1, V2, V3> PType<Tuple3.Collect<V1, V2, V3>> derived(PType<V1> first,
        PType<V2> second, PType<V3> third) {
      PTypeFamily tf = first.getFamily();
      PType<Tuple3<Collection<V1>, Collection<V2>, Collection<V3>>> pt = 
          tf.triples(
              tf.collections(first),
              tf.collections(second),
              tf.collections(third));
      Object clazz = Tuple3.Collect.class;
      return tf.derived((Class<Tuple3.Collect<V1, V2, V3>>) clazz,
          new MapFn<Tuple3<Collection<V1>, Collection<V2>, Collection<V3>>, Collect<V1, V2, V3>>() {
        @Override
        public Collect<V1, V2, V3> map(
            Tuple3<Collection<V1>, Collection<V2>, Collection<V3>> in) {
          return new Collect<V1, V2, V3>(in.first(), in.second(), in.third());
        }
      },
      new MapFn<Collect<V1, V2, V3>, Tuple3<Collection<V1>, Collection<V2>, Collection<V3>>>() {
        @Override
        public Tuple3<Collection<V1>, Collection<V2>, Collection<V3>> map(
            Collect<V1, V2, V3> in) {
          return in;
        }
      }, pt);
    }
    
    public Collect(Collection<V1> first, Collection<V2> second, Collection<V3> third) {
      super(first, second, third);
    }
  }
  
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
    Tuple3<?, ?, ?> other = (Tuple3<?, ?, ?>) obj;
    return (first == other.first || (first != null && first.equals(other.first)))
        && (second == other.second || (second != null && second.equals(other.second)))
        && (third == other.third || (third != null && third.equals(other.third)));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Tuple3[");
    sb.append(first).append(",").append(second).append(",").append(third);
    return sb.append("]").toString();
  }
}
