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

import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.DoFn;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.Source;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.run.NodeContext;
import org.apache.crunch.impl.mr.run.RTNode;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class DoNode {

  private static final List<DoNode> NO_CHILDREN = ImmutableList.of();

  private final DoFn fn;
  private final String name;
  private final PType<?> ptype;
  private final List<DoNode> children;
  private final Converter outputConverter;
  private final Source<?> source;
  private final ParallelDoOptions options;
  private String outputName;

  private DoNode(DoFn fn, String name, PType<?> ptype, List<DoNode> children, Converter outputConverter,
      Source<?> source, ParallelDoOptions options) {
    this.fn = fn;
    this.name = name;
    this.ptype = ptype;
    this.children = children;
    this.outputConverter = outputConverter;
    this.source = source;
    this.options = options;
  }

  private static List<DoNode> allowsChildren() {
    return Lists.newArrayList();
  }

  public static <K, V> DoNode createGroupingNode(String name, PGroupedTableType<K, V> ptype) {
    Converter groupingConverter = ptype.getGroupingConverter();
    DoFn<?, ?> fn = groupingConverter.applyPTypeTransforms() ? ptype.getOutputMapFn() : IdentityFn.getInstance();
    return new DoNode(fn, name, ptype, NO_CHILDREN, ptype.getGroupingConverter(), null, null);
  }

  public static DoNode createOutputNode(String name, Converter outputConverter, PType<?> ptype) {
    DoFn<?, ?> fn = outputConverter.applyPTypeTransforms() ? ptype.getOutputMapFn() : IdentityFn.getInstance();
    return new DoNode(fn, name, ptype, NO_CHILDREN, outputConverter, null, null);
  }

  public static DoNode createFnNode(String name, DoFn<?, ?> function, PType<?> ptype, ParallelDoOptions options) {
    return new DoNode(function, name, ptype, allowsChildren(), null, null, options);
  }

  public static <S> DoNode createInputNode(Source<S> source) {
    Converter srcConverter = source.getConverter();
    PType<?> ptype = source.getType();
    DoFn<?, ?> fn = srcConverter.applyPTypeTransforms() ? ptype.getInputMapFn() : IdentityFn.getInstance();
    return new DoNode(fn, source.toString(), ptype, allowsChildren(), null, source, null);
  }

  public boolean isOutputNode() {
    return outputConverter != null;
  }

  public String getName() {
    return name;
  }

  public List<DoNode> getChildren() {
    return children;
  }

  public Source<?> getSource() {
    return source;
  }

  public PType<?> getPType() {
    return ptype;
  }

  public DoNode addChild(DoNode node) {
    // TODO: This is sort of terrible, refactor the code to make this make more sense.
    boolean exists = false;
    for (DoNode child : children) {
      if (node == child || (node.isOutputNode() && node.equals(child))) {
        exists = true;
        break;
      }
    }
    if (!exists) {
      children.add(node);
    }
    return this;
  }

  public void setOutputName(String outputName) {
    if (outputConverter == null) {
      throw new IllegalStateException("Cannot set output name w/o output converter: " + outputName);
    }
    this.outputName = outputName;
  }

  public RTNode toRTNode(boolean inputNode, Configuration conf, NodeContext nodeContext) {
    List<RTNode> childRTNodes = Lists.newArrayList();
    if (options != null) {
      options.configure(conf);
    }
    fn.configure(conf);
    for (DoNode child : children) {
      childRTNodes.add(child.toRTNode(false, conf, nodeContext));
    }

    Converter inputConverter = null;
    if (inputNode) {
      if (nodeContext == NodeContext.MAP) {
        inputConverter = source.getConverter();
      } else {
        inputConverter = ((PGroupedTableType<?, ?>) ptype).getGroupingConverter();
      }
    }
    return new RTNode(fn, (PType<Object>) getPType(), name, childRTNodes, inputConverter, outputConverter, outputName);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof DoNode)) {
      return false;
    }
    if (this == other) {
      return true;
    }
    DoNode o = (DoNode) other;
    return name.equals(o.name) && fn.equals(o.fn) && source == o.source && outputConverter == o.outputConverter;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(name).append(fn).append(source).append(outputConverter).toHashCode();
  }
}
