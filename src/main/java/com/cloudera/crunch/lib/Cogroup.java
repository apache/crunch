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
package com.cloudera.crunch.lib;

import java.util.Collection;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.fn.MapValuesFn;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import com.google.common.collect.Lists;

public class Cogroup {
  
  /**
   * Co-groups the two {@link PTable} arguments.
   * 
   * @return a {@code PTable} representing the co-grouped tables.
   */
  public static <K, U, V> PTable<K, Pair<Collection<U>, Collection<V>>> cogroup(
      PTable<K, U> left, PTable<K, V> right) {
    PTypeFamily ptf = left.getTypeFamily();
    PType<K> keyType = left.getPTableType().getKeyType();
    PType<U> leftType = left.getPTableType().getValueType();
    PType<V> rightType = right.getPTableType().getValueType();
    PType<Pair<U, V>> itype = ptf.pairs(leftType, rightType);

    PTable<K, Pair<U, V>> cgLeft = left.parallelDo("coGroupTag1",
        new CogroupFn1<K, U, V>(), ptf.tableOf(keyType, itype));
    PTable<K, Pair<U, V>> cgRight = right.parallelDo("coGroupTag2",
        new CogroupFn2<K, U, V>(), ptf.tableOf(keyType, itype));
    
    PTable<K, Pair<U, V>> both = cgLeft.union(cgRight);

    PType<Pair<Collection<U>, Collection<V>>> otype = ptf.pairs(
        ptf.collections(leftType), ptf.collections(rightType));
    return both.groupByKey().parallelDo("cogroup",
        new PostGroupFn<K, U, V>(), ptf.tableOf(keyType, otype));
  }

  private static class CogroupFn1<K, V, U> extends MapValuesFn<K, V, Pair<V, U>> {
    @Override
    public Pair<V, U> map(V v) {
      return Pair.of(v, null);
    }
  }

  private static class CogroupFn2<K, V, U> extends MapValuesFn<K, U, Pair<V, U>> {
    @Override
    public Pair<V, U> map(U u) {
      return Pair.of(null, u);
    }
  }

  private static class PostGroupFn<K, V, U> extends
      DoFn<Pair<K, Iterable<Pair<V, U>>>, Pair<K, Pair<Collection<V>, Collection<U>>>> {
    @Override
    public void process(Pair<K, Iterable<Pair<V, U>>> input,
        Emitter<Pair<K, Pair<Collection<V>, Collection<U>>>> emitter) {
      Collection<V> cv = Lists.newArrayList();
      Collection<U> cu = Lists.newArrayList();
      for (Pair<V, U> pair : input.second()) {
        if (pair.first() != null) {
          cv.add(pair.first());
        } else if (pair.second() != null) {
          cu.add(pair.second());
        }
      }
      emitter.emit(Pair.of(input.first(), Pair.of(cv, cu)));
    }
  }

}
