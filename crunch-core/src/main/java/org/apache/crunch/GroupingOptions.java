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
package org.apache.crunch;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.crunch.impl.mr.run.UniformHashPartitioner;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Options that can be passed to a {@code groupByKey} operation in order to
 * exercise finer control over how the partitioning, grouping, and sorting of
 * keys is performed.
 * 
 */
public class GroupingOptions implements Serializable {

  private final Class<? extends Partitioner> partitionerClass;
  private final Class<? extends RawComparator> groupingComparatorClass;
  private final Class<? extends RawComparator> sortComparatorClass;
  private final boolean requireSortedKeys;
  private final int numReducers;
  private final Map<String, String> extraConf;
  private transient Set<SourceTarget<?>> sourceTargets;
  
  private GroupingOptions(Class<? extends Partitioner> partitionerClass,
      Class<? extends RawComparator> groupingComparatorClass, Class<? extends RawComparator> sortComparatorClass,
      boolean requireSortedKeys, int numReducers,
      Map<String, String> extraConf,
      Set<SourceTarget<?>> sourceTargets) {
    this.partitionerClass = partitionerClass;
    this.groupingComparatorClass = groupingComparatorClass;
    this.sortComparatorClass = sortComparatorClass;
    this.requireSortedKeys = requireSortedKeys;
    this.numReducers = numReducers;
    this.extraConf = extraConf;
    this.sourceTargets = sourceTargets;
  }

  public int getNumReducers() {
    return numReducers;
  }

  public boolean requireSortedKeys() {
    return requireSortedKeys;
  }

  public Class<? extends RawComparator> getSortComparatorClass() {
    return sortComparatorClass;
  }

  public Class<? extends RawComparator> getGroupingComparatorClass() {
    return groupingComparatorClass;
  }
  
  public Class<? extends Partitioner> getPartitionerClass() {
    return partitionerClass;
  }
  
  public Set<SourceTarget<?>> getSourceTargets() {
    return sourceTargets;
  }
  
  public void configure(Job job) {
    if (partitionerClass != null) {
      job.setPartitionerClass(partitionerClass);
    }
    if (groupingComparatorClass != null) {
      job.setGroupingComparatorClass(groupingComparatorClass);
    }
    if (sortComparatorClass != null) {
      job.setSortComparatorClass(sortComparatorClass);
    }
    if (numReducers > 0) {
      job.setNumReduceTasks(numReducers);
    }
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      job.getConfiguration().set(e.getKey(), e.getValue());
    }
  }

  public boolean isCompatibleWith(GroupingOptions other) {
    if (partitionerClass != other.partitionerClass) {
      return false;
    }
    if (groupingComparatorClass != other.groupingComparatorClass) {
      return false;
    }
    if (sortComparatorClass != other.sortComparatorClass) {
      return false;
    }
    if (!extraConf.equals(other.extraConf)) {
      return false;
    }
    return true;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for creating {@code GroupingOptions} instances.
   * 
   */
  public static class Builder {
    private Class<? extends Partitioner> partitionerClass = UniformHashPartitioner.class;
    private Class<? extends RawComparator> groupingComparatorClass;
    private Class<? extends RawComparator> sortComparatorClass;
    private boolean requireSortedKeys;
    private int numReducers;
    private Map<String, String> extraConf = Maps.newHashMap();
    private Set<SourceTarget<?>> sourceTargets = Sets.newHashSet();
    
    public Builder() {
    }

    public Builder partitionerClass(Class<? extends Partitioner> partitionerClass) {
      this.partitionerClass = partitionerClass;
      return this;
    }

    public Builder groupingComparatorClass(Class<? extends RawComparator> groupingComparatorClass) {
      this.groupingComparatorClass = groupingComparatorClass;
      return this;
    }

    public Builder sortComparatorClass(Class<? extends RawComparator> sortComparatorClass) {
      this.sortComparatorClass = sortComparatorClass;
      return this;
    }

    public Builder requireSortedKeys() {
      requireSortedKeys = true;
      return this;
    }

    public Builder numReducers(int numReducers) {
      if (numReducers <= 0) {
        throw new IllegalArgumentException("Invalid number of reducers: " + numReducers);
      }
      this.numReducers = numReducers;
      return this;
    }

    public Builder conf(String confKey, String confValue) {
      this.extraConf.put(confKey, confValue);
      return this;
    }

    @Deprecated
    public Builder sourceTarget(SourceTarget<?> st) {
      this.sourceTargets.add(st);
      return this;
    }

    public Builder sourceTargets(SourceTarget<?>... st) {
      Collections.addAll(this.sourceTargets, st);
      return this;
    }

    public Builder sourceTargets(Collection<SourceTarget<?>> st) {
      this.sourceTargets.addAll(st);
      return this;
    }

    public GroupingOptions build() {
      return new GroupingOptions(partitionerClass, groupingComparatorClass, sortComparatorClass,
          requireSortedKeys, numReducers, extraConf, sourceTargets);
    }
  }
}
