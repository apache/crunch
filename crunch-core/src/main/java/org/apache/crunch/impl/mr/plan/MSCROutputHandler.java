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
package org.apache.crunch.impl.mr.plan;

import java.util.Map;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Target;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.PathTarget;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.Maps;

public class MSCROutputHandler implements OutputHandler {

  private final Job job;
  private final Path path;
  private final boolean mapOnlyJob;

  private DoNode workingNode;
  private Map<Integer, PathTarget> multiPaths;
  private int jobCount;

  public MSCROutputHandler(Job job, Path outputPath, boolean mapOnlyJob) {
    this.job = job;
    this.path = outputPath;
    this.mapOnlyJob = mapOnlyJob;
    this.multiPaths = Maps.newHashMap();
  }

  public void configureNode(DoNode node, Target target) {
    workingNode = node;
    if (!target.accept(this, node.getPType())) {
      throw new CrunchRuntimeException("Target " + target + " cannot serialize PType of class: " +
          node.getPType().getClass());
    }
  }

  public boolean configure(Target target, PType<?> ptype) {
    if (target instanceof MapReduceTarget) {
      if (target instanceof PathTarget) {
        multiPaths.put(jobCount, (PathTarget) target);
      }

      String name = PlanningParameters.MULTI_OUTPUT_PREFIX + jobCount;
      jobCount++;
      workingNode.setOutputName(name);
      ((MapReduceTarget) target).configureForMapReduce(job, ptype, path, name);
      return true;
    }

    return false;
  }

  public boolean isMapOnlyJob() {
    return mapOnlyJob;
  }

  public Map<Integer, PathTarget> getMultiPaths() {
    return multiPaths;
  }
}
