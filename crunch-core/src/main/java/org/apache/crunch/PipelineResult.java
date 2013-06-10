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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import java.util.List;

/**
 * Container for the results of a call to {@code run} or {@code done} on the
 * Pipeline interface that includes details and statistics about the component
 * stages of the data pipeline.
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

  public static final PipelineResult EMPTY = new PipelineResult(ImmutableList.<StageResult> of(), PipelineExecution.Status.READY);

  private final List<StageResult> stageResults;

  public PipelineExecution.Status status;

  public PipelineResult(List<StageResult> stageResults, PipelineExecution.Status status) {
    this.stageResults = ImmutableList.copyOf(stageResults);
    this.status = status;
  }

  public boolean succeeded() {
    // return !stageResults.isEmpty();
    return this.status.equals(PipelineExecution.Status.SUCCEEDED);
  }

  public List<StageResult> getStageResults() {
    return stageResults;
  }
}
