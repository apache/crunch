/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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

import java.util.List;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import com.google.common.collect.ImmutableList;

/**
 * Container for the results of a call to {@code run} or {@code done} on the Pipeline interface that includes
 * details and statistics about the component stages of the data pipeline.
 */
public class PipelineResult {

  public static class StageResult {
    
    private final String stageName;
    private final Counters counters;
    
    public StageResult(String stageName, Counters counters) {
      this.stageName = stageName;
      this.counters = counters;
    }
    
    public String getStageName() {
      return stageName;
    }
    
    public Counters getCounters() {
      return counters;
    }
    
    public Counter findCounter(Enum<?> key) {
      return counters.findCounter(key);
    }
    
    public long getCounterValue(Enum<?> key) {
      return findCounter(key).getValue();
    }
  }
  
  public static final PipelineResult EMPTY = new PipelineResult(ImmutableList.<StageResult>of());
  
  private final List<StageResult> stageResults;
  
  public PipelineResult(List<StageResult> stageResults) {
    this.stageResults = ImmutableList.copyOf(stageResults);
  }
  
  public boolean succeeded() {
    return !stageResults.isEmpty();
  }
  
  public List<StageResult> getStageResults() {
    return stageResults;
  }
}
