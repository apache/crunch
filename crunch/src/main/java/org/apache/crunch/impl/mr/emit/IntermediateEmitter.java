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
package org.apache.crunch.impl.mr.emit;

import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.impl.mr.run.RTNode;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;

/**
 * An {@link Emitter} implementation that links the output of one {@link DoFn} to the input of
 * another {@code DoFn}.
 * 
 */
public class IntermediateEmitter implements Emitter<Object> {

  private final List<RTNode> children;
  private final Configuration conf;
  private final PType<Object> outputPType;
  private final boolean needDetachedValues;

  public IntermediateEmitter(PType<Object> outputPType, List<RTNode> children, Configuration conf) {
    this.outputPType = outputPType;
    this.children = ImmutableList.copyOf(children);
    this.conf = conf;

    outputPType.initialize(conf);
    needDetachedValues = this.children.size() > 1;
  }

  public void emit(Object emitted) {
    for (RTNode child : children) {
      Object value = emitted;
      if (needDetachedValues) {
        value = this.outputPType.getDetachedValue(emitted);
      }
      child.process(value);
    }
  }

  public void flush() {
    // No-op
  }
}
