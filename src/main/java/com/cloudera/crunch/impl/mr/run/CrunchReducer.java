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
import org.apache.hadoop.mapreduce.Reducer;

public class CrunchReducer extends Reducer<Object, Object, Object, Object> {

  private static final Log LOG = LogFactory.getLog(CrunchReducer.class);
  
  private RTNode node;
  private CrunchTaskContext ctxt;
  private boolean debug;
  
  protected NodeContext getNodeContext() {
    return NodeContext.REDUCE;
  }
  
  @Override
  protected void setup(Reducer<Object, Object, Object, Object>.Context context) {
    this.ctxt = new CrunchTaskContext(context, getNodeContext());
    try {
      List<RTNode> nodes = ctxt.getNodes();
      this.node = nodes.get(0);
    } catch (IOException e) {
      LOG.info("Crunch deserialization error", e);
      throw new CrunchRuntimeException(e);
    }
    this.debug = ctxt.isDebugRun();
  }

  @Override
  protected void reduce(Object key, Iterable<Object> values,
      Reducer<Object, Object, Object, Object>.Context context) {
    if (debug) {
      try {
        node.processIterable(key, values);
      } catch (Exception e) {
        LOG.error("Reducer exception", e);
      }
    } else {
      node.processIterable(key, values);
    }
  }

  @Override
  protected void cleanup(Reducer<Object, Object, Object, Object>.Context context) {
    node.cleanup();
    ctxt.cleanup();
  }
}
