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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.impl.SingleUseIterable;
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
    if (ctxt == null) {
      this.ctxt = new CrunchTaskContext(context, getNodeContext());
      this.debug = ctxt.isDebugRun();
    }
    this.node = ctxt.getNodes().get(0);
    this.node.initialize(ctxt);
  }

  @Override
  protected void reduce(Object key, Iterable<Object> values, Reducer<Object, Object, Object, Object>.Context context) {
    values = new SingleUseIterable<Object>(values);
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
