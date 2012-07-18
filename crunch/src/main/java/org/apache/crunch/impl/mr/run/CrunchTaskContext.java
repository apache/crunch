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
package org.apache.crunch.impl.mr.run;

import java.io.IOException;
import java.util.List;

import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.crunch.hadoop.mapreduce.lib.output.CrunchMultipleOutputs;

public class CrunchTaskContext {

  private final TaskInputOutputContext<Object, Object, Object, Object> taskContext;
  private final NodeContext nodeContext;
  private CrunchMultipleOutputs<Object, Object> multipleOutputs;

  public CrunchTaskContext(TaskInputOutputContext<Object, Object, Object, Object> taskContext, NodeContext nodeContext) {
    this.taskContext = taskContext;
    this.nodeContext = nodeContext;
  }

  public TaskInputOutputContext<Object, Object, Object, Object> getContext() {
    return taskContext;
  }

  public NodeContext getNodeContext() {
    return nodeContext;
  }

  public List<RTNode> getNodes() throws IOException {
    Configuration conf = taskContext.getConfiguration();
    Path path = new Path(new Path(conf.get(PlanningParameters.CRUNCH_WORKING_DIRECTORY)), nodeContext.toString());
    @SuppressWarnings("unchecked")
    List<RTNode> nodes = (List<RTNode>) DistCache.read(conf, path);
    if (nodes != null) {
      for (RTNode node : nodes) {
        node.initialize(this);
      }
    }
    return nodes;
  }

  public boolean isDebugRun() {
    Configuration conf = taskContext.getConfiguration();
    return conf.getBoolean(RuntimeParameters.DEBUG, false);
  }

  public void cleanup() {
    if (multipleOutputs != null) {
      try {
        multipleOutputs.close();
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      } catch (InterruptedException e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  public CrunchMultipleOutputs<Object, Object> getMultipleOutputs() {
    if (multipleOutputs == null) {
      multipleOutputs = new CrunchMultipleOutputs<Object, Object>(taskContext);
    }
    return multipleOutputs;
  }
}
