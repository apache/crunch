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

import org.apache.crunch.GroupingOptions;
import org.apache.crunch.impl.dist.collect.BaseGroupedTable;
import org.apache.crunch.impl.dist.collect.MRCollection;
import org.apache.crunch.impl.dist.collect.PTableBase;
import org.apache.crunch.impl.mr.plan.DoNode;
import org.apache.crunch.util.PartitionUtils;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PGroupedTableImpl<K, V> extends BaseGroupedTable<K, V> implements MRCollection {

  private static final Logger LOG = LoggerFactory.getLogger(PGroupedTableImpl.class);

  PGroupedTableImpl(PTableBase<K, V> parent, GroupingOptions groupingOptions) {
    super(parent, groupingOptions);
  }

  public void configureShuffle(Job job) {
    ptype.configureShuffle(job, groupingOptions);
    if (groupingOptions == null || groupingOptions.getNumReducers() <= 0) {
      int numReduceTasks = PartitionUtils.getRecommendedPartitions(this, getPipeline().getConfiguration());
      if (numReduceTasks > 0) {
        job.setNumReduceTasks(numReduceTasks);
        LOG.info("Setting num reduce tasks to {}", numReduceTasks);
      } else {
        LOG.warn("Attempted to set a negative number of reduce tasks");
      }
    }
  }

  @Override
  public void accept(Visitor visitor) {
    visitor.visitGroupedTable(this);
  }

  @Override
  public DoNode createDoNode() {
    return DoNode.createFnNode(getName(), ptype.getInputMapFn(), ptype, doOptions);
  }

  public DoNode getGroupingNode() {
    return DoNode.createGroupingNode("", ptype);
  }
}
