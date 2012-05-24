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
package com.cloudera.crunch.impl.mr.plan;

import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Source;
import com.cloudera.crunch.impl.mr.run.NodeContext;
import com.cloudera.crunch.impl.mr.run.RTNode;
import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.PGroupedTableType;
import com.cloudera.crunch.types.PType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class DoNode {

  private static final List<DoNode> NO_CHILDREN = ImmutableList.of();

  private final DoFn fn;
  private final String name;
  private final PType ptype;
  private final List<DoNode> children;
  private final Converter outputConverter;
  private final Source source;
  private String outputName;

  private DoNode(DoFn fn, String name, PType ptype, List<DoNode> children,
      Converter outputConverter, Source source) {
    this.fn = fn;
    this.name = name;
    this.ptype = ptype;
    this.children = children;
    this.outputConverter = outputConverter;
    this.source = source;
  }

  private static List<DoNode> allowsChildren() {
    return Lists.newArrayList();
  }

  public static <K, V> DoNode createGroupingNode(String name,
      PGroupedTableType<K, V> ptype) {
    DoFn fn = ptype.getOutputMapFn();
    return new DoNode(fn, name, ptype, NO_CHILDREN,
        ptype.getGroupingConverter(), null);
  }
  
  public static <S> DoNode createOutputNode(String name, PType<S> ptype) {
    Converter outputConverter = ptype.getConverter();
    DoFn fn = ptype.getOutputMapFn();
    return new DoNode(fn, name, ptype, NO_CHILDREN,
        outputConverter, null);
  }

  public static DoNode createFnNode(String name, DoFn<?, ?> function,
      PType<?> ptype) {
    return new DoNode(function, name, ptype, allowsChildren(), null, null);
  }

  public static <S> DoNode createInputNode(Source<S> source) {
    PType ptype = source.getType();
    DoFn fn = ptype.getInputMapFn();
    return new DoNode(fn, source.toString(), ptype, allowsChildren(), null,
        source);
  }

  public boolean isInputNode() {
    return source != null;
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
  
  public Source getSource() {
    return source;
  }

  public PType getPType() {
    return ptype;
  }

  public DoNode addChild(DoNode node) {
    if (!children.contains(node)) {
      this.children.add(node);
    }
    return this;
  }

  public void setOutputName(String outputName) {
    if (outputConverter == null) {
      throw new IllegalStateException(
          "Cannot set output name w/o output converter: " + outputName);
    }
    this.outputName = outputName;
  }

  public RTNode toRTNode(boolean inputNode, Configuration conf, NodeContext nodeContext) {
    List<RTNode> childRTNodes = Lists.newArrayList();
    fn.configure(conf);
    for (DoNode child : children) {
      childRTNodes.add(child.toRTNode(false, conf, nodeContext));
    }

    Converter inputConverter = null;
    if (inputNode) {
      if (nodeContext == NodeContext.MAP) {
        inputConverter = ptype.getConverter();
      } else {
        inputConverter = ((PGroupedTableType) ptype).getGroupingConverter();
      }
    }          
    return new RTNode(fn, name, childRTNodes, inputConverter, outputConverter,
        outputName);
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof DoNode)) {
      return false;
    }
    if (this == other) {
      return true;
    }
    DoNode o = (DoNode) other;
    return (name.equals(o.name) && fn.equals(o.fn) && source == o.source &&
        outputConverter == o.outputConverter);
  }
  
  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(name).append(fn).append(source)
        .append(outputConverter).toHashCode();
  }
}
