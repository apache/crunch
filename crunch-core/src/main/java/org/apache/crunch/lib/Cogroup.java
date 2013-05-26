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

import java.util.Collection;

import org.apache.crunch.MapFn;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.collect.Lists;

public class Cogroup {

  /**
   * Co-groups the two {@link PTable} arguments.
   * 
   * @param left The left (smaller) PTable
   * @param right The right (larger) PTable
   * @return a {@code PTable} representing the co-grouped tables
   */
  public static <K, U, V> PTable<K, Pair<Collection<U>, Collection<V>>> cogroup(PTable<K, U> left, PTable<K, V> right) {
    return cogroup(left, right, 0);
  }
  
  /**
   * Co-groups the two {@link PTable} arguments with a user-specified degree of parallelism (a.k.a, number of
   * reducers.)
   * 
   * @param left The left (smaller) PTable
   * @param right The right (larger) PTable
   * @param numReducers The number of reducers to use
   * @return A new {@code PTable} representing the co-grouped tables
   */
  public static <K, U, V> PTable<K, Pair<Collection<U>, Collection<V>>> cogroup(
      PTable<K, U> left,
      PTable<K, V> right,
      int numReducers) {
    PTypeFamily ptf = left.getTypeFamily();
    PType<U> leftType = left.getPTableType().getValueType();
    PType<V> rightType = right.getPTableType().getValueType();
    PType<Pair<U, V>> itype = ptf.pairs(leftType, rightType);

    PTable<K, Pair<U, V>> cgLeft = left.mapValues("coGroupTag1", new CogroupFn1<U, V>(),
        itype);
    PTable<K, Pair<U, V>> cgRight = right.mapValues("coGroupTag2", new CogroupFn2<U, V>(),
        itype);

    PType<Pair<Collection<U>, Collection<V>>> otype = ptf.pairs(ptf.collections(leftType),
        ptf.collections(rightType));
    PTable<K, Pair<U, V>> both = cgLeft.union(cgRight);
    PGroupedTable<K, Pair<U, V>> grouped = null;
    if (numReducers > 0) {
      grouped = both.groupByKey(numReducers);
    } else {
      grouped = both.groupByKey();
    }
    return grouped.mapValues("cogroup", new PostGroupFn<U, V>(leftType, rightType), otype);
  }

  private static class CogroupFn1<V, U> extends MapFn<V, Pair<V, U>> {
    @Override
    public Pair<V, U> map(V v) {
      return Pair.of(v, null);
    }
  }

  private static class CogroupFn2<V, U> extends MapFn<U, Pair<V, U>> {
    @Override
    public Pair<V, U> map(U u) {
      return Pair.of(null, u);
    }
  }

  private static class PostGroupFn<V, U> extends
      MapFn<Iterable<Pair<V, U>>, Pair<Collection<V>, Collection<U>>> {
    
    private PType<V> ptypeV;
    private PType<U> ptypeU;
    
    public PostGroupFn(PType<V> ptypeV, PType<U> ptypeU) {
      this.ptypeV = ptypeV;
      this.ptypeU = ptypeU;
    }
    
    @Override
    public void initialize() {
      super.initialize();
      ptypeV.initialize(getConfiguration());
      ptypeU.initialize(getConfiguration());
    }
    
    @Override
    public Pair<Collection<V>, Collection<U>> map(Iterable<Pair<V, U>> input) {
      Collection<V> cv = Lists.newArrayList();
      Collection<U> cu = Lists.newArrayList();
      for (Pair<V, U> pair : input) {
        if (pair.first() != null) {
          cv.add(ptypeV.getDetachedValue(pair.first()));
        } else if (pair.second() != null) {
          cu.add(ptypeU.getDetachedValue(pair.second()));
        }
      }
      return Pair.of(cv, cu);
    }
  }

}
