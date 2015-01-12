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
package org.apache.crunch.lib;

import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.fn.PairMapFn;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.collect.Lists;

/**
 * Methods for performing common operations on PTables.
 * 
 */
public class PTables {

  /**
   * Convert the given {@code PCollection<Pair<K, V>>} to a {@code PTable<K, V>}.
   * @param pcollect The {@code PCollection} to convert
   * @return A {@code PTable} that contains the same data as the input {@code PCollection}
   */
  public static <K, V> PTable<K, V> asPTable(PCollection<Pair<K, V>> pcollect) {
    PType<Pair<K, V>> pt = pcollect.getPType();
    PTypeFamily ptf = pt.getFamily();
    PTableType<K, V> ptt = ptf.tableOf(pt.getSubTypes().get(0), pt.getSubTypes().get(1));
    DoFn<Pair<K, V>, Pair<K, V>> id = IdentityFn.getInstance();
    return pcollect.parallelDo("asPTable", id, ptt);
  }

  /**
   * Maps a {@code PTable<K1, V>} to a {@code PTable<K2, V>} using the given {@code MapFn<K1, K2>} on
   * the keys of the {@code PTable}.
   * 
   * @param ptable The {@code PTable} to be mapped
   * @param mapFn The mapping function
   * @param ptype The PType for the returned keys
   * @return A new {@code PTable<K2, V>} instance
   */
  public static <K1, K2, V> PTable<K2, V> mapKeys(PTable<K1, V> ptable, MapFn<K1, K2> mapFn,
      PType<K2> ptype) {
    return mapKeys("PTables.mapKeys", ptable, mapFn, ptype);
  }
  
  /**
   * Maps a {@code PTable<K1, V>} to a {@code PTable<K2, V>} using the given {@code MapFn<K1, K2>} on
   * the keys of the {@code PTable}.
   * 
   * @param name The name of the transform
   * @param ptable The {@code PTable} to be mapped
   * @param mapFn The mapping function
   * @param ptype The PType for the returned keys
   * @return A new {@code PTable<K2, V>} instance
   */
  public static <K1, K2, V> PTable<K2, V> mapKeys(String name, PTable<K1, V> ptable, MapFn<K1, K2> mapFn,
      PType<K2> ptype) {
    PTypeFamily ptf = ptable.getTypeFamily();
    return ptable.parallelDo(name,
        new PairMapFn<K1, V, K2, V>(mapFn, IdentityFn.<V>getInstance()),
        ptf.tableOf(ptype, ptable.getValueType()));
  }
  
  /**
   * Maps a {@code PTable<K, U>} to a {@code PTable<K, V>} using the given {@code MapFn<U, V>} on
   * the values of the {@code PTable}.
   * 
   * @param ptable The {@code PTable} to be mapped
   * @param mapFn The mapping function
   * @param ptype The PType for the returned values
   * @return A new {@code PTable<K, V>} instance
   */
  public static <K, U, V> PTable<K, V> mapValues(PTable<K, U> ptable, MapFn<U, V> mapFn,
      PType<V> ptype) {
    return mapValues("PTables.mapValues", ptable, mapFn, ptype);
  }
  
  /**
   * Maps a {@code PTable<K, U>} to a {@code PTable<K, V>} using the given {@code MapFn<U, V>} on
   * the values of the {@code PTable}.
   * 
   * @param name The name of the transform
   * @param ptable The {@code PTable} to be mapped
   * @param mapFn The mapping function
   * @param ptype The PType for the returned values
   * @return A new {@code PTable<K, V>} instance
   */
  public static <K, U, V> PTable<K, V> mapValues(String name, PTable<K, U> ptable, MapFn<U, V> mapFn,
      PType<V> ptype) {
    PTypeFamily ptf = ptable.getTypeFamily();
    return ptable.parallelDo(name,
        new PairMapFn<K, U, K, V>(IdentityFn.<K>getInstance(), mapFn),
        ptf.tableOf(ptable.getKeyType(), ptype));
  }
  
  /**
   * An analogue of the {@code mapValues} function for {@code PGroupedTable<K, U>} collections.
   * 
   * @param ptable The {@code PGroupedTable} to be mapped
   * @param mapFn The mapping function
   * @param ptype The PType for the returned values
   * @return A new {@code PTable<K, V>} instance
   */
  public static <K, U, V> PTable<K, V> mapValues(PGroupedTable<K, U> ptable,
      MapFn<Iterable<U>, V> mapFn,
      PType<V> ptype) {
    return mapValues("PTables.mapValues", ptable, mapFn, ptype);
  }
  
  /**
   * An analogue of the {@code mapValues} function for {@code PGroupedTable<K, U>} collections.
   * 
   * @param name The name of the operation
   * @param ptable The {@code PGroupedTable} to be mapped
   * @param mapFn The mapping function
   * @param ptype The PType for the returned values
   * @return A new {@code PTable<K, V>} instance
   */
  public static <K, U, V> PTable<K, V> mapValues(String name,
      PGroupedTable<K, U> ptable,
      MapFn<Iterable<U>, V> mapFn,
      PType<V> ptype) {
    PTypeFamily ptf = ptable.getTypeFamily();
    return ptable.parallelDo(name,
        new PairMapFn<K, Iterable<U>, K, V>(IdentityFn.<K>getInstance(), mapFn),
        ptf.tableOf((PType<K>) ptable.getPType().getSubTypes().get(0), ptype));
  }
  
  /**
   * Extract the keys from the given {@code PTable<K, V>} as a {@code PCollection<K>}.
   * @param ptable The {@code PTable}
   * @return A {@code PCollection<K>}
   */
  public static <K, V> PCollection<K> keys(PTable<K, V> ptable) {
    return ptable.parallelDo("PTables.keys", new DoFn<Pair<K, V>, K>() {
      @Override
      public void process(Pair<K, V> input, Emitter<K> emitter) {
        emitter.emit(input.first());
      }
    }, ptable.getKeyType());
  }

  /**
   * Extract the values from the given {@code PTable<K, V>} as a {@code PCollection<V>}.
   * @param ptable The {@code PTable}
   * @return A {@code PCollection<V>}
   */
  public static <K, V> PCollection<V> values(PTable<K, V> ptable) {
    return ptable.parallelDo("PTables.values", new DoFn<Pair<K, V>, V>() {
      @Override
      public void process(Pair<K, V> input, Emitter<V> emitter) {
        emitter.emit(input.second());
      }
    }, ptable.getValueType());
  }

  /**
   * Create a detached value for a table {@link Pair}.
   * 
   * @param tableType The table type
   * @param value The value from which a detached value is to be created
   * @return The detached value
   * @see PType#getDetachedValue(Object)
   */
  public static <K, V> Pair<K, V> getDetachedValue(PTableType<K, V> tableType, Pair<K, V> value) {
    return Pair.of(tableType.getKeyType().getDetachedValue(value.first()), tableType.getValueType()
        .getDetachedValue(value.second()));
  }

  /**
   * Created a detached value for a {@link PGroupedTable} value.
   * 
   * 
   * @param groupedTableType The grouped table type
   * @param value The value from which a detached value is to be created
   * @return The detached value
   * @see PType#getDetachedValue(Object)
   */
  public static <K, V> Pair<K, Iterable<V>> getGroupedDetachedValue(
      PGroupedTableType<K, V> groupedTableType, Pair<K, Iterable<V>> value) {

    PTableType<K, V> tableType = groupedTableType.getTableType();
    List<V> detachedIterable = Lists.newArrayList();
    PType<V> valueType = tableType.getValueType();
    for (V v : value.second()) {
      detachedIterable.add(valueType.getDetachedValue(v));
    }
    return Pair.of(tableType.getKeyType().getDetachedValue(value.first()),
        (Iterable<V>) detachedIterable);
  }

  /**
   * Swap the key and value part of a table. The original PTypes are used in the opposite order
   * @param table PTable to process
   * @param <K> Key type (will become value type)
   * @param <V> Value type (will become key type)
   * @return PType&lt;V, K&gt; containing the same data as the original
   */
  public static <K, V> PTable<V, K> swapKeyValue(PTable<K, V> table) {
    PTypeFamily ptf = table.getTypeFamily();
    return table.parallelDo(new MapFn<Pair<K, V>, Pair<V, K>>() {
      @Override
      public Pair<V, K> map(Pair<K, V> input) {
        return Pair.of(input.second(), input.first());
      }
    }, ptf.tableOf(table.getValueType(), table.getKeyType()));
  }
}
