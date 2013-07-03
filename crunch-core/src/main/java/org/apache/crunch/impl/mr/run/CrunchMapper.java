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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class CrunchMapper extends Mapper<Object, Object, Object, Object> {

  private static final Log LOG = LogFactory.getLog(CrunchMapper.class);

  private RTNode node;
  private CrunchTaskContext ctxt;
  private boolean debug;

  @Override
  protected void setup(Mapper<Object, Object, Object, Object>.Context context) {
    if (ctxt == null) {
      ctxt = new CrunchTaskContext(context, NodeContext.MAP);
      this.debug = ctxt.isDebugRun();
    }
    
    List<RTNode> nodes = ctxt.getNodes();
    if (nodes.size() == 1) {
      this.node = nodes.get(0);
    } else {
      CrunchInputSplit split = (CrunchInputSplit) context.getInputSplit();
      this.node = nodes.get(split.getNodeIndex());
    }
    this.node.initialize(ctxt);
  }

  @Override
  protected void map(Object k, Object v, Mapper<Object, Object, Object, Object>.Context context) {
    if (debug) {
      try {
        node.process(k, v);
      } catch (Exception e) {
        LOG.error("Mapper exception", e);
      }
    } else {
      node.process(k, v);
    }
  }

  @Override
  protected void cleanup(Mapper<Object, Object, Object, Object>.Context context) {
    node.cleanup();
    ctxt.cleanup();
  }
}
