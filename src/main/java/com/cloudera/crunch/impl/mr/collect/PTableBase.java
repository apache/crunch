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
package com.cloudera.crunch.impl.mr.collect;

import java.util.List;

import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Target;
import com.cloudera.crunch.type.PType;
import com.google.common.collect.Lists;

public abstract class PTableBase<K, V> extends PCollectionImpl<Pair<K, V>>
    implements PTable<K, V> {

  public PTableBase(String name) {
    super(name);
  }

  public PType<K> getKeyType() {
    return getPTableType().getKeyType();
  }
  
  public PType<V> getValueType() {
    return getPTableType().getValueType();
  }
  
  public PGroupedTableImpl<K, V> groupByKey() {
    return new PGroupedTableImpl<K, V>(this);
  }

  public PGroupedTableImpl<K, V> groupByKey(int numReduceTasks) {
    return new PGroupedTableImpl<K, V>(this,
        GroupingOptions.builder().numReducers(numReduceTasks).build());
  }
  
  public PGroupedTableImpl<K, V> groupByKey(GroupingOptions groupingOptions) {
    return new PGroupedTableImpl<K, V>(this, groupingOptions);
  }

  @Override
  public PTable<K, V> union(PTable<K, V>... others) {
    List<PTableBase<K, V>> internal = Lists.newArrayList();
    internal.add(this);
    for (PTable<K, V> table : others) {
      internal.add((PTableBase<K, V>) table);
    }
    return new UnionTable<K, V>(internal);
  }
  
  @Override
  public PTable<K, V> write(Target target) {
    getPipeline().write(this, target);
    return this;
  }
}
