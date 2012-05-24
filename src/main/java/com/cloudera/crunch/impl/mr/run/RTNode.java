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

import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.impl.mr.emit.IntermediateEmitter;
import com.cloudera.crunch.impl.mr.emit.MultipleOutputEmitter;
import com.cloudera.crunch.impl.mr.emit.OutputEmitter;
import com.cloudera.crunch.types.Converter;

public class RTNode implements Serializable {
  
  private static final Log LOG = LogFactory.getLog(RTNode.class);
  
  private final String nodeName;
  private DoFn<Object, Object> fn;
  private final List<RTNode> children;
  private final Converter inputConverter;
  private final Converter outputConverter;
  private final String outputName;

  private transient Emitter emitter;

  public RTNode(DoFn<Object, Object> fn, String name, List<RTNode> children,
      Converter inputConverter, Converter outputConverter, String outputName) {
    this.fn = fn;
    this.nodeName = name;
    this.children = children;
    this.inputConverter = inputConverter;
    this.outputConverter = outputConverter;
    this.outputName = outputName;
  }

  public void initialize(CrunchTaskContext ctxt) {
    if (emitter != null) {
      // Already initialized
      return;
    }
    
    fn.setContext(ctxt.getContext());
    for (RTNode child : children) {
      child.initialize(ctxt);
    }

    if (outputConverter != null) {
      if (outputName != null) {
        this.emitter = new MultipleOutputEmitter(
            outputConverter, ctxt.getMultipleOutputs(), outputName);
      } else {
        this.emitter = new OutputEmitter(
            outputConverter, ctxt.getContext());
      }
    } else if (!children.isEmpty()) {
      this.emitter = new IntermediateEmitter(children);
    } else {
      throw new CrunchRuntimeException("Invalid RTNode config: no emitter for: " + nodeName);
    }
  }

  public boolean isLeafNode() {
    return outputConverter != null && children.isEmpty();
  }

  public void process(Object input) {
    try {
      fn.process(input, emitter);
    } catch (CrunchRuntimeException e) {
      if (!e.wasLogged()) {
        LOG.info(String.format("Crunch exception in '%s' for input: %s",
            nodeName, input.toString()), e);
        e.markLogged();
      }
      throw e;
    }
  }

  public void process(Object key, Object value) {
    process(inputConverter.convertInput(key, value));
  }

  public void processIterable(Object key, Iterable values) {
    process(inputConverter.convertIterableInput(key, values));
  }
  
  public void cleanup() {
    fn.cleanup(emitter);
    emitter.flush();
    for (RTNode child : children) {
      child.cleanup();
    }
  }

  @Override
  public String toString() {
    return "RTNode [nodeName=" + nodeName + ", fn=" + fn + ", children="
        + children + ", inputConverter=" + inputConverter
        + ", outputConverter=" + outputConverter + ", outputName=" + outputName
        + "]";
  }
}
