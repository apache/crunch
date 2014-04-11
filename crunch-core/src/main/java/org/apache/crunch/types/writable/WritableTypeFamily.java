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
package org.apache.crunch.types.writable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.Union;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.PTypeUtils;
import org.apache.hadoop.io.Writable;

/**
 * The {@link Writable}-based implementation of the
 * {@link org.apache.crunch.types.PTypeFamily} interface.
 */
public class WritableTypeFamily implements PTypeFamily {

  private static final WritableTypeFamily INSTANCE = new WritableTypeFamily();

  public static WritableTypeFamily getInstance() {
    return INSTANCE;
  }

  // Disallow construction
  private WritableTypeFamily() {
  }

  public PType<Void> nulls() {
    return Writables.nulls();
  }

  public PType<String> strings() {
    return Writables.strings();
  }

  public PType<Long> longs() {
    return Writables.longs();
  }

  public PType<Integer> ints() {
    return Writables.ints();
  }

  public PType<Float> floats() {
    return Writables.floats();
  }

  public PType<Double> doubles() {
    return Writables.doubles();
  }

  public PType<Boolean> booleans() {
    return Writables.booleans();
  }

  public PType<ByteBuffer> bytes() {
    return Writables.bytes();
  }

  public <T> PType<T> records(Class<T> clazz) {
    return Writables.records(clazz);
  }

  public <W extends Writable> PType<W> writables(Class<W> clazz) {
    return Writables.writables(clazz);
  }

  public <K, V> PTableType<K, V> tableOf(PType<K> key, PType<V> value) {
    return Writables.tableOf(key, value);
  }

  public <V1, V2> PType<Pair<V1, V2>> pairs(PType<V1> p1, PType<V2> p2) {
    return Writables.pairs(p1, p2);
  }

  public <V1, V2, V3> PType<Tuple3<V1, V2, V3>> triples(PType<V1> p1, PType<V2> p2, PType<V3> p3) {
    return Writables.triples(p1, p2, p3);
  }

  public <V1, V2, V3, V4> PType<Tuple4<V1, V2, V3, V4>> quads(PType<V1> p1, PType<V2> p2, PType<V3> p3, PType<V4> p4) {
    return Writables.quads(p1, p2, p3, p4);
  }

  public PType<TupleN> tuples(PType<?>... ptypes) {
    return Writables.tuples(ptypes);
  }

  public <T> PType<Collection<T>> collections(PType<T> ptype) {
    return Writables.collections(ptype);
  }

  public <T> PType<Map<String, T>> maps(PType<T> ptype) {
    return Writables.maps(ptype);
  }

  @Override
  public <T> PType<T> as(PType<T> ptype) {
    if (ptype instanceof WritableType || ptype instanceof WritableTableType
        || ptype instanceof WritableGroupedTableType) {
      return ptype;
    }
    if (ptype instanceof PGroupedTableType) {
      PTableType ptt = ((PGroupedTableType) ptype).getTableType();
      return new WritableGroupedTableType((WritableTableType) as(ptt));
    }
    PType<T> prim = Writables.getPrimitiveType(ptype.getTypeClass());
    if (prim != null) {
      return prim;
    }
    return PTypeUtils.convert(ptype, this);
  }

  @Override
  public <T extends Tuple> PType<T> tuples(Class<T> clazz, PType<?>... ptypes) {
    return Writables.tuples(clazz, ptypes);
  }

  @Override
  public <S, T> PType<T> derived(Class<T> clazz, MapFn<S, T> inputFn, MapFn<T, S> outputFn, PType<S> base) {
    return Writables.derived(clazz, inputFn, outputFn, base);
  }

  @Override
  public <S, T> PType<T> derivedImmutable(Class<T> clazz, MapFn<S, T> inputFn, MapFn<T, S> outputFn, PType<S> base) {
    return Writables.derivedImmutable(clazz, inputFn, outputFn, base);
  }

  @Override
  public PType<Union> unionOf(PType<?>... ptypes) {
    return Writables.unionOf(ptypes);
  }
}
