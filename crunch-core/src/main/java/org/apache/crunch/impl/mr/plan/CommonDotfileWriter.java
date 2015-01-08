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

import static java.lang.String.format;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.ArrayList;

import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.types.PType;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Common Debug dotfile writer class. Provides the draw abstraction common for all debug dotfile writers.
 */
@SuppressWarnings({ "rawtypes" })
public abstract class CommonDotfileWriter {

  protected static final String DEFAULT_FOLDER_COLOR = "darkGreen";

  protected static final String[] COLORS = new String[] { "blue", "red", "green", "yellow", "cyan", "darkGray", "gray",
      "magenta", "darkGreen", "black" };

  protected StringBuilder edgeBuilder = null;
  protected StringBuilder contentBuilder = null;

  protected String label(String text) {
    return text == null ? "-" : text;
  }

  protected String className(Object obj) {

    if (obj == null) {
      return "-";
    }

    Class clazz = null;
    if (obj instanceof Class) {
      clazz = (Class) obj;
    } else {
      clazz = obj.getClass();
    }
    String s = clazz.getName();
    s = s.substring(s.lastIndexOf('.') + 1);

    return s;
  }

  protected String getPCollectionID(PCollectionImpl<?> pcollectionImpl) {
    return String.format("\"%s@%d\"", pcollectionImpl.getName(), pcollectionImpl.hashCode());
  }

  protected String formatPCollection(PCollectionImpl<?> pcollectionImpl) {

    String withBreakpoint = pcollectionImpl.isBreakpoint() ? " [breakpoint]" : "";

    return String.format("%s [label=\"{%s | %s | %s }\", shape=%s, color=%s];\n", getPCollectionID(pcollectionImpl),
        pcollectionImpl.getName(), className(pcollectionImpl) + withBreakpoint,
        formatPType(pcollectionImpl.getPType()), "record", "black");
  }

  protected String formatPType(PType ptype) {

    StringBuilder sb = new StringBuilder();

    sb.append(className(ptype.getTypeClass()));

    if (!isEmpty(ptype.getSubTypes())) {

      ArrayList<String> subtypes = Lists.newArrayList();
      for (Object subType : ptype.getSubTypes()) {
        if (subType instanceof PType) {
          subtypes.add(formatPType((PType) subType));
        } else {
          subtypes.add(className(subType));
        }
      }

      sb.append("[").append(Joiner.on(", ").join(subtypes)).append("]");
    }

    return sb.toString();
  }

  private String getSourceID(Source s) {
    return "\"ST@" + s + "\"";
  }

  private String getTargetID(Target t) {
    return "\"ST@" + t + "\"";
  }

  protected void formatTarget(Target target, String color) {
    contentBuilder.append(String.format("%s [label=\"%s\", shape=folder, color=\"%s\"];\n", getTargetID(target),
        target.toString(), color));
  }

  protected void formatSource(Source source, String color) {
    contentBuilder.append(String.format("%s [label=\"%s\", shape=folder, color=\"%s\"];\n", getSourceID(source),
        source.toString(), color));
  }

  protected void link(String from, String to, String color) {
    edgeBuilder.append(String.format("%s -> %s [color=\"%s\"];\n", from, to, color));
  }

  protected void link(PCollectionImpl pc, Target target, String color) {
    link(getPCollectionID(pc), getTargetID(target), color);
  }

  protected void link(PCollectionImpl parent, PCollectionImpl child, String color) {
    link(getPCollectionID(parent), getPCollectionID(child), color);
  }

  protected void link(Source source, PCollectionImpl pc, String color) {
    link(getSourceID(source), getPCollectionID(pc), color);
  }

  public String buildDiagram(String diagramName) {

    edgeBuilder = new StringBuilder();
    contentBuilder = new StringBuilder();

    contentBuilder.append("digraph G {\n");
    contentBuilder.append(format("   label=\"%s \\n\\n\"; fontsize=24; labelloc=\"t\"; \n", diagramName));

    contentBuilder.append(getLgentd());

    try {
      doBuildDiagram();
    } catch (Throwable t) {
      contentBuilder.append("\"" + Throwables.getRootCause(t) + "\"");
    }

    contentBuilder.append(edgeBuilder);
    contentBuilder.append("}\n");

    return contentBuilder.toString();
  }

  public String getLgentd() {
    StringBuilder lsb = new StringBuilder();
    lsb.append("subgraph \"cluster-legend-rtnodes\" {\n").append(
        "label=\"LEGEND\" ; fontsize=10; style=filled; color=lightblue;\n");

    doGetLegend(lsb);

    lsb.append("}\n");
    return lsb.toString();
  }

  protected abstract void doBuildDiagram();

  protected abstract void doGetLegend(StringBuilder lsb);
}
