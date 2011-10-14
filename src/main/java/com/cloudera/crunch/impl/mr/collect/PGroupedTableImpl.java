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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.CombineFn;
import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.impl.mr.plan.DoNode;
import com.cloudera.crunch.type.PGroupedTableType;
import com.cloudera.crunch.type.PType;
import com.google.common.collect.ImmutableList;

public class PGroupedTableImpl<K, V> extends
    PCollectionImpl<Pair<K, Iterable<V>>> implements PGroupedTable<K, V> {

  private static final Log LOG = LogFactory.getLog(PGroupedTableImpl.class);
  
  private final PTableBase<K, V> parent;
  private final GroupingOptions groupingOptions;
  private final PGroupedTableType<K, V> ptype;

  PGroupedTableImpl(PTableBase<K, V> parent) {
    this(parent, null);
  }

  PGroupedTableImpl(PTableBase<K, V> parent, GroupingOptions groupingOptions) {
    super("Grouped(" + parent.getName() + ")");
    this.parent = parent;
    this.groupingOptions = groupingOptions;
    this.ptype = parent.getPTableType().getGroupedTableType();
  }

  public void configureShuffle(Job job) {
    ptype.configureShuffle(job, groupingOptions);
    if (groupingOptions != null && groupingOptions.getNumReducers() <= 0) {
      int bytesPerTask = job.getConfiguration().getInt("crunch.bytes.per.reduce.task",
          (1000 * 1000 * 1000));
      int numReduceTasks = 1 + (int) (getSize() / bytesPerTask);
      job.setNumReduceTasks(numReduceTasks);
      LOG.info(String.format("Setting num reduce tasks to %d", numReduceTasks));
    }
  }
  
  @Override
  public long getSize() {
    return parent.getSize();
  }
  
  @Override
  public PType<Pair<K, Iterable<V>>> getPType() {
    return ptype;
  }

  public PTable<K, V> combineValues(CombineFn<K, V> combineFn) {
    return new DoTableImpl<K, V>(getCombineTableName(), this, combineFn,
        parent.getPTableType());
  }

  private String getCombineTableName() {
    return "combine(" + getName() + ")";
  }

  public PTable<K, V> ungroup() {
    return parallelDo("ungroup(" + getName() + ")",
        new Ungroup<K, V>(), parent.getPTableType());
  }

  private static class Ungroup<K, V> extends DoFn<Pair<K, Iterable<V>>, Pair<K, V>> {
    @Override
    public void process(Pair<K, Iterable<V>> input, Emitter<Pair<K, V>> emitter) {
      for (V v : input.second()) {
        emitter.emit(Pair.of(input.first(), v));
      }
    }
  }

  @Override
  protected void acceptInternal(PCollectionImpl.Visitor visitor) {
    visitor.visitGroupedTable(this);
  }

  @Override
  public List<PCollectionImpl<?>> getParents() {
    return ImmutableList.<PCollectionImpl<?>> of(parent);
  }

  @Override
  public DoNode createDoNode() {
    return DoNode.createFnNode("R:" + getName(),
        ptype.getGroupingBridge().getInputMapFn(), ptype);
  }

  public DoNode getGroupingNode() {
    return DoNode.createGroupingNode("M:" + getName(),
        ptype);
  }
}
