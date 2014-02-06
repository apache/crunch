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

import com.google.common.collect.Lists;
import org.apache.crunch.MapFn;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.Union;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.TupleFactory;

import java.util.Collection;

public class Cogroup {

  /**
   * Co-groups the two {@link PTable} arguments.
   * 
   * @param left The left (smaller) PTable
   * @param right The right (larger) PTable
   * @return a {@code PTable} representing the co-grouped tables
   */
  public static <K, U, V> PTable<K, Pair<Collection<U>, Collection<V>>> cogroup(PTable<K, U> left, PTable<K, V> right) {
    return cogroup(0, left, right);
  }
  
  /**
   * Co-groups the two {@link PTable} arguments with a user-specified degree of parallelism (a.k.a, number of
   * reducers.)
   * 
   * @param numReducers The number of reducers to use
   * @param left The left (smaller) PTable
   * @param right The right (larger) PTable
   * @return A new {@code PTable} representing the co-grouped tables
   */
  public static <K, U, V> PTable<K, Pair<Collection<U>, Collection<V>>> cogroup(
      int numReducers,
      PTable<K, U> left,
      PTable<K, V> right) {
    PTypeFamily tf = left.getTypeFamily();
    return cogroup(
        tf.pairs(tf.collections(left.getValueType()),
                 tf.collections(right.getValueType())),
        TupleFactory.PAIR,
        numReducers,
        left, right);
  }

  /**
   * Co-groups the three {@link PTable} arguments.
   * 
   * @param first The smallest PTable
   * @param second The second-smallest PTable
   * @param third The largest PTable
   * @return a {@code PTable} representing the co-grouped tables
   */
  public static <K, V1, V2, V3> PTable<K, Tuple3.Collect<V1, V2, V3>> cogroup(
      PTable<K, V1> first,
      PTable<K, V2> second,
      PTable<K, V3> third) {
    return cogroup(0, first, second, third);
  }
  
  /**
   * Co-groups the three {@link PTable} arguments with a user-specified degree of parallelism (a.k.a, number of
   * reducers.)
   * 
   * @param numReducers The number of reducers to use
   * @param first The smallest PTable
   * @param second The second-smallest PTable
   * @param third The largest PTable
   * @return A new {@code PTable} representing the co-grouped tables
   */
  public static <K, V1, V2, V3> PTable<K, Tuple3.Collect<V1, V2, V3>> cogroup(
      int numReducers,
      PTable<K, V1> first,
      PTable<K, V2> second,
      PTable<K, V3> third) {
    return cogroup(
        Tuple3.Collect.derived(first.getValueType(), second.getValueType(), third.getValueType()),
        new TupleFactory<Tuple3.Collect<V1, V2, V3>>() {
          @Override
          public Tuple3.Collect<V1, V2, V3> makeTuple(Object... values) {
            return new Tuple3.Collect<V1, V2, V3>(
                (Collection<V1>) values[0],
                (Collection<V2>) values[1],
                (Collection<V3>) values[2]);
          }
        },
        numReducers,
        first, second, third);
  }
  
  /**
   * Co-groups the three {@link PTable} arguments.
   * 
   * @param first The smallest PTable
   * @param second The second-smallest PTable
   * @param third The largest PTable
   * @return a {@code PTable} representing the co-grouped tables
   */
  public static <K, V1, V2, V3, V4> PTable<K, Tuple4.Collect<V1, V2, V3, V4>> cogroup(
      PTable<K, V1> first,
      PTable<K, V2> second,
      PTable<K, V3> third,
      PTable<K, V4> fourth) {
    return cogroup(0, first, second, third, fourth);
  }
  
  /**
   * Co-groups the three {@link PTable} arguments with a user-specified degree of parallelism (a.k.a, number of
   * reducers.)
   * 
   * @param numReducers The number of reducers to use
   * @param first The smallest PTable
   * @param second The second-smallest PTable
   * @param third The largest PTable
   * @return A new {@code PTable} representing the co-grouped tables
   */
  public static <K, V1, V2, V3, V4> PTable<K, Tuple4.Collect<V1, V2, V3, V4>> cogroup(
      int numReducers,
      PTable<K, V1> first,
      PTable<K, V2> second,
      PTable<K, V3> third,
      PTable<K, V4> fourth) {
    return cogroup(
        Tuple4.Collect.derived(first.getValueType(), second.getValueType(), third.getValueType(),
            fourth.getValueType()),
        new TupleFactory<Tuple4.Collect<V1, V2, V3, V4>>() {
          @Override
          public Tuple4.Collect<V1, V2, V3, V4> makeTuple(Object... values) {
            return new Tuple4.Collect<V1, V2, V3, V4>(
                (Collection<V1>) values[0],
                (Collection<V2>) values[1],
                (Collection<V3>) values[2],
                (Collection<V4>) values[3]);
          }
        },
        numReducers,
        first, second, third, fourth);
  }
  
  /**
   * Co-groups an arbitrary number of {@link PTable} arguments. The largest table should
   * come last in the ordering.
   * 
   * @param first The first (smallest) PTable to co-group
   * @param rest The other (larger) PTables to co-group
   * @return a {@code PTable} representing the co-grouped tables
   */
  public static <K> PTable<K, TupleN> cogroup(PTable<K, ?> first, PTable<K, ?>... rest) {
    return cogroup(0, first, rest);
  }
  
  /**
   * Co-groups an arbitrary number of {@link PTable} arguments with a user-specified degree of parallelism
   * (a.k.a, number of reducers.) The largest table should come last in the ordering.
   * 
   * @param numReducers The number of reducers to use
   * @param first The first (smallest) PTable to co-group
   * @param rest The other (larger) PTables to co-group
   * @return A new {@code PTable} representing the co-grouped tables
   */
  public static <K, U, V> PTable<K, TupleN> cogroup(
      int numReducers,
      PTable<K, ?> first,
      PTable<K, ?>... rest) {
    PTypeFamily tf = first.getTypeFamily();
    PType[] components = new PType[1 + rest.length];
    components[0] = tf.collections(first.getValueType());
    for (int i = 0; i < rest.length; i++) {
      components[i + 1] = tf.collections(rest[i].getValueType());
    }
    return cogroup(
        tf.tuples(components),
        TupleFactory.TUPLEN,
        numReducers,
        first, rest);
  }
  
  private static <K, T extends Tuple> PTable<K, T> cogroup(
      PType<T> outputType,
      TupleFactory tupleFactory,
      int numReducers,
      PTable<K, ?> first, PTable<K, ?>... rest) {
    PTypeFamily ptf = first.getTypeFamily();
    PType[] ptypes = new PType[1 + rest.length];
    ptypes[0] = first.getValueType();
    for (int i = 0; i < rest.length; i++) {
      ptypes[i + 1] = rest[i].getValueType();
    }
    PType<Union> itype = ptf.unionOf(ptypes);
    
    PTable<K, Union> firstInter = first.mapValues("coGroupTag1",
        new CogroupFn(0), itype);
    PTable<K, Union>[] inter = new PTable[rest.length];
    for (int i = 0; i < rest.length; i++) {
      inter[i] = rest[i].mapValues("coGroupTag" + (i + 2),
          new CogroupFn(i + 1), itype);
    }
    
    PTable<K, Union> union = firstInter.union(inter);
    PGroupedTable<K, Union> grouped;
    if (numReducers > 0) {
      grouped = union.groupByKey(numReducers);
    } else {
      grouped = union.groupByKey();
    }
    
    return grouped.mapValues("cogroup", 
        new PostGroupFn<T>(tupleFactory, ptypes),
        outputType);
  }
  
  private static class CogroupFn<T> extends MapFn<T, Union> {
    private final int index;

    CogroupFn(int index) {
      this.index = index;
    }

    @Override
    public Union map(T input) {
      return new Union(index, input);
    }
  }

  private static class PostGroupFn<T extends Tuple> extends
      MapFn<Iterable<Union>, T> {
    
    private final TupleFactory factory;
    private final PType[] ptypes;
    
    PostGroupFn(TupleFactory tf, PType... ptypes) {
      this.factory = tf;
      this.ptypes = ptypes;
    }
    
    @Override
    public void initialize() {
      super.initialize();
      for (PType pt : ptypes) {
        pt.initialize(getConfiguration());
      }
    }
    
    @Override
    public T map(Iterable<Union> input) {
      Collection[] collections = new Collection[ptypes.length];
      for (int i = 0; i < ptypes.length; i++) {
        collections[i] = Lists.newArrayList();
      }
      for (Union t : input) {
        int index = t.getIndex();
        collections[index].add(ptypes[index].getDetachedValue(t.getValue()));
      }
      return (T) factory.makeTuple(collections);
    }
  }

}
