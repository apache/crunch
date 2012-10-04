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
package org.apache.crunch.impl.mr.collect;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.plan.DoNode;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.ImmutableList;

public class PGroupedTableImpl<K, V> extends PCollectionImpl<Pair<K, Iterable<V>>> implements PGroupedTable<K, V> {

  private static final Log LOG = LogFactory.getLog(PGroupedTableImpl.class);

  private final PTableBase<K, V> parent;
  private final GroupingOptions groupingOptions;
  private final PGroupedTableType<K, V> ptype;
  
  PGroupedTableImpl(PTableBase<K, V> parent) {
    this(parent, null);
  }

  PGroupedTableImpl(PTableBase<K, V> parent, GroupingOptions groupingOptions) {
    super("GBK");
    this.parent = parent;
    this.groupingOptions = groupingOptions;
    this.ptype = parent.getPTableType().getGroupedTableType();
  }

  public void configureShuffle(Job job) {
    ptype.configureShuffle(job, groupingOptions);
    if (groupingOptions == null || groupingOptions.getNumReducers() <= 0) {
      long bytesPerTask = job.getConfiguration().getLong("crunch.bytes.per.reduce.task", (1000L * 1000L * 1000L));
      int numReduceTasks = 1 + (int) (getSize() / bytesPerTask);
      if (numReduceTasks > 0) {
        job.setNumReduceTasks(numReduceTasks);
        LOG.info(String.format("Setting num reduce tasks to %d", numReduceTasks));
      } else {
        LOG.warn("Attempted to set a negative number of reduce tasks");
      }
    }
  }

  @Override
  protected long getSizeInternal() {
    return parent.getSizeInternal();
  }

  @Override
  public PType<Pair<K, Iterable<V>>> getPType() {
    return ptype;
  }

  public PTable<K, V> combineValues(CombineFn<K, V> combineFn) {
    return new DoTableImpl<K, V>("combine", getChainingCollection(), combineFn, parent.getPTableType());
  }

  private static class Ungroup<K, V> extends DoFn<Pair<K, Iterable<V>>, Pair<K, V>> {
    @Override
    public void process(Pair<K, Iterable<V>> input, Emitter<Pair<K, V>> emitter) {
      for (V v : input.second()) {
        emitter.emit(Pair.of(input.first(), v));
      }
    }
  }

  public PTable<K, V> ungroup() {
    return parallelDo("ungroup", new Ungroup<K, V>(), parent.getPTableType());
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
    return DoNode.createFnNode(getName(), ptype.getInputMapFn(), ptype);
  }

  public DoNode getGroupingNode() {
    return DoNode.createGroupingNode("", ptype);
  }
  
  @Override
  protected PCollectionImpl<Pair<K, Iterable<V>>> getChainingCollection() {
    // Use a copy for chaining to allow sending the output of a single grouped table to multiple outputs
    // TODO This should be implemented in a cleaner way in the planner
    return new PGroupedTableImpl<K, V>(parent, groupingOptions);
  }
}
