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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
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

    /**
     * @deprecated The {@link Counter} class changed incompatibly between Hadoop 1 and 2
     * (from a class to an interface) so user programs should avoid this method and use
     * {@link #getCounterNames()}.
     */
    @Deprecated
    public Counters getCounters() {
      return counters;
    }

    /**
     * @return a map of group names to counter names.
     */
    public Map<String, Set<String>> getCounterNames() {
      Map<String, Set<String>> names = Maps.newHashMap();
      for (CounterGroup counterGroup : counters) {
        Set<String> counterNames = Sets.newHashSet();
        for (Counter counter : counterGroup) {
          counterNames.add(counter.getName());
        }
        names.put(counterGroup.getName(), counterNames);
      }
      return names;
    }

    /**
     * @deprecated The {@link Counter} class changed incompatibly between Hadoop 1 and 2
     * (from a class to an interface) so user programs should avoid this method and use
     * {@link #getCounterValue(Enum)} and/or {@link #getCounterDisplayName(Enum)}.
     */
    @Deprecated
    public Counter findCounter(Enum<?> key) {
      return counters.findCounter(key);
    }

    public long getCounterValue(String groupName, String counterName) {
      return counters.findCounter(groupName, counterName).getValue();
    }

    public String getCounterDisplayName(String groupName, String counterName) {
      return counters.findCounter(groupName, counterName).getDisplayName();
    }

    public long getCounterValue(Enum<?> key) {
      return counters.findCounter(key).getValue();
    }

    public String getCounterDisplayName(Enum<?> key) {
      return counters.findCounter(key).getDisplayName();
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
