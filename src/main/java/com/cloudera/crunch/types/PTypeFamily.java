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

package com.cloudera.crunch.types;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Tuple;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.Tuple4;
import com.cloudera.crunch.TupleN;

/**
 * An abstract factory for creating {@code PType} instances that have the same
 * serialization/storage backing format.
 * 
 */
public interface PTypeFamily {
  PType<Void> nulls();
  
  PType<String> strings();

  PType<Long> longs();

  PType<Integer> ints();

  PType<Float> floats();

  PType<Double> doubles();

  PType<Boolean> booleans();
  
  PType<ByteBuffer> bytes();
  
  <T> PType<T> records(Class<T> clazz);

  <T> PType<Collection<T>> collections(PType<T> ptype);

  <T> PType<Map<String, T>> maps(PType<T> ptype);
  
  <V1, V2> PType<Pair<V1, V2>> pairs(PType<V1> p1, PType<V2> p2);

  <V1, V2, V3> PType<Tuple3<V1, V2, V3>> triples(PType<V1> p1, PType<V2> p2,
      PType<V3> p3);

  <V1, V2, V3, V4> PType<Tuple4<V1, V2, V3, V4>> quads(PType<V1> p1,
      PType<V2> p2, PType<V3> p3, PType<V4> p4);

  PType<TupleN> tuples(PType... ptypes);

  <T extends Tuple> PType<T> tuples(Class<T> clazz, PType... ptypes);
  
  <S, T> PType<T> derived(Class<T> clazz, MapFn<S, T> inputFn, MapFn<T, S> outputFn, PType<S> base);
  
  <K, V> PTableType<K, V> tableOf(PType<K> key, PType<V> value);
  
  /**
   * Returns the equivalent of the given ptype for this family, if it exists.
   */
  <T> PType<T> as(PType<T> ptype);  
}
