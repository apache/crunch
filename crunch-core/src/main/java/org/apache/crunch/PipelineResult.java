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
import com.google.common.collect.ImmutableMap;
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
    private final String stageId;
    private final Counters counters;
    private final long startTimeMsec;
    private final long jobStartTimeMsec;
    private final long jobEndTimeMsec;
    private final long endTimeMsec;

    public StageResult(String stageName, Counters counters) {
      this(stageName, counters, System.currentTimeMillis(), System.currentTimeMillis());
    }

    public StageResult(String stageName, Counters counters, long startTimeMsec, long endTimeMsec) {
      this(stageName, stageName, counters, startTimeMsec, startTimeMsec, endTimeMsec, endTimeMsec);
    }

    public StageResult(String stageName, String stageId, Counters counters, long startTimeMsec,
        long jobStartTimeMsec, long jobEndTimeMsec, long endTimeMsec) {
      this.stageName = stageName;
      this.stageId = stageId;
      this.counters = counters;
      this.startTimeMsec = startTimeMsec;
      this.jobStartTimeMsec = jobStartTimeMsec;
      this.jobEndTimeMsec = jobEndTimeMsec;
      this.endTimeMsec = endTimeMsec;
    }

    public String getStageName() {
      return stageName;
    }

    public String getStageId() {
      return stageId;
    }

    /**
     * @return the overall start time for this stage, that is, the time at which any pre-job hooks were
     * started.
     */
    public long getStartTimeMsec() {
      return startTimeMsec;
    }

    /**
     * @return the time that the work for this stage was submitted to the cluster for execution, if applicable.
     */
    public long getJobStartTimeMsec() {
      return jobStartTimeMsec;
    }

    /**
     * @return the time that the work for this stage finished processing on the cluster, if applicable.
     */
    public long getJobEndTimeMsec() {
      return jobEndTimeMsec;
    }

    /**
     * @return the overall end time for this stage, that is, the time at which any post-job hooks completed.
     */
    public long getEndTimeMsec() {
      return endTimeMsec;
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
      if (counters == null) {
        return ImmutableMap.of();
      }
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
      if (counters == null) {
        return null;
      }
      return counters.findCounter(key);
    }

    public long getCounterValue(String groupName, String counterName) {
      if (counters == null) {
        return 0L;
      }
      return counters.findCounter(groupName, counterName).getValue();
    }

    public String getCounterDisplayName(String groupName, String counterName) {
      if (counters == null) {
        return null;
      }
      return counters.findCounter(groupName, counterName).getDisplayName();
    }

    public long getCounterValue(Enum<?> key) {
      if (counters == null) {
        return 0L;
      }
      return counters.findCounter(key).getValue();
    }

    public String getCounterDisplayName(Enum<?> key) {
      if (counters == null) {
        return null;
      }
      return counters.findCounter(key).getDisplayName();
    }
  }

  public static final PipelineResult EMPTY = new PipelineResult(ImmutableList.<StageResult> of(), PipelineExecution.Status.READY);
  public static final PipelineResult DONE = new PipelineResult(ImmutableList.<StageResult> of(), PipelineExecution.Status.SUCCEEDED);

  private final List<StageResult> stageResults;

  public PipelineExecution.Status status;

  public PipelineResult(List<StageResult> stageResults, PipelineExecution.Status status) {
    this.stageResults = ImmutableList.copyOf(stageResults);
    this.status = status;
  }

  public boolean succeeded() {
    // return !stageResults.isEmpty();
    return this.status == PipelineExecution.Status.SUCCEEDED;
  }

  public List<StageResult> getStageResults() {
    return stageResults;
  }
}
