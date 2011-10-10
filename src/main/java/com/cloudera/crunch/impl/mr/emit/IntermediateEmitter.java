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
package com.cloudera.crunch.impl.mr.emit;

import java.util.List;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.impl.mr.run.RTNode;
import com.google.common.collect.ImmutableList;

/**
 * An {@link Emitter} implementation that links the output of one {@link DoFn}
 * to the input of another {@code DoFn}.
 * 
 */
public class IntermediateEmitter implements Emitter<Object> {

  private final List<RTNode> children;

  public IntermediateEmitter(List<RTNode> children) {
    this.children = ImmutableList.copyOf(children);
  }

  public void emit(Object emitted) {
    for (RTNode child : children) {
      child.process(emitted);
    }
  }

  public void flush() {
    // No-op
  }
}
