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
package com.cloudera.crunch.impl.mr.run;

import java.io.IOException;
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
    List<RTNode> nodes;
    this.ctxt = new CrunchTaskContext(context, NodeContext.MAP);
    try {
      nodes = ctxt.getNodes();
    } catch (IOException e) {
      LOG.info("Crunch deserialization error", e);
      throw new CrunchRuntimeException(e);
    }
    if (nodes.size() == 1) {
      this.node = nodes.get(0);
    } else {
      CrunchInputSplit split = (CrunchInputSplit) context.getInputSplit();
      this.node = nodes.get(split.getNodeIndex());
    }
    this.debug = ctxt.isDebugRun();
  }

  @Override
  protected void map(Object k, Object v,
      Mapper<Object, Object, Object, Object>.Context context) {
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
