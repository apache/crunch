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

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

class CrunchTaskContext {

  private final TaskInputOutputContext<Object, Object, Object, Object> taskContext;
  private final NodeContext nodeContext;
  private final List<RTNode> nodes;
  private CrunchOutputs<Object, Object> multipleOutputs;
  
  public CrunchTaskContext(TaskInputOutputContext<Object, Object, Object, Object> taskContext,
      NodeContext nodeContext) {
    this.taskContext = taskContext;
    this.nodeContext = nodeContext;
    Configuration conf = taskContext.getConfiguration();
    Path path = new Path(nodeContext.toString());
    try {
      this.nodes = (List<RTNode>) DistCache.read(conf, path);
    } catch (IOException e) {
      throw new CrunchRuntimeException("Could not read runtime node information", e);
    }
  }

  public TaskInputOutputContext<Object, Object, Object, Object> getContext() {
    return taskContext;
  }

  public NodeContext getNodeContext() {
    return nodeContext;
  }

  public List<RTNode> getNodes() {
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

  public CrunchOutputs<Object, Object> getMultipleOutputs() {
    if (multipleOutputs == null) {
      multipleOutputs = new CrunchOutputs<Object, Object>(taskContext);
    }
    return multipleOutputs;
  }
}
