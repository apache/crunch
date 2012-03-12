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

package com.cloudera.crunch;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Options that can be passed to a {@code groupByKey} operation in order to exercise
 * finer control over how the partitioning, grouping, and sorting of keys is
 * performed.
 *
 */
public class GroupingOptions {

  private final Class<? extends Partitioner> partitionerClass;
  private final Class<? extends RawComparator> groupingComparatorClass;
  private final Class<? extends RawComparator> sortComparatorClass;
  private final int numReducers;

  private GroupingOptions(Class<? extends Partitioner> partitionerClass,
      Class<? extends RawComparator> groupingComparatorClass,
      Class<? extends RawComparator> sortComparatorClass, int numReducers) {
    this.partitionerClass = partitionerClass;
    this.groupingComparatorClass = groupingComparatorClass;
    this.sortComparatorClass = sortComparatorClass;
    this.numReducers = numReducers;
  }

  public int getNumReducers() {
    return numReducers;
  }
  
  public Class<? extends RawComparator> getSortComparatorClass() {
    return sortComparatorClass;
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
    private Class<? extends Partitioner> partitionerClass;
    private Class<? extends RawComparator> groupingComparatorClass;
    private Class<? extends RawComparator> sortComparatorClass;
    private int numReducers;

    public Builder() {
    }

    public Builder partitionerClass(
        Class<? extends Partitioner> partitionerClass) {
      this.partitionerClass = partitionerClass;
      return this;
    }

    public Builder groupingComparatorClass(
        Class<? extends RawComparator> groupingComparatorClass) {
      this.groupingComparatorClass = groupingComparatorClass;
      return this;
    }

    public Builder sortComparatorClass(
        Class<? extends RawComparator> sortComparatorClass) {
      this.sortComparatorClass = sortComparatorClass;
      return this;
    }

    public Builder numReducers(int numReducers) {
      if (numReducers <= 0) {
        throw new IllegalArgumentException("Invalid number of reducers: " + numReducers);
      }
      this.numReducers = numReducers;
      return this;
    }

    public GroupingOptions build() {
      return new GroupingOptions(partitionerClass, groupingComparatorClass,
          sortComparatorClass, numReducers);
    }
  }
}
