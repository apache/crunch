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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

@SuppressWarnings("rawtypes")
public class DotfileWriterGraph extends CommonDotfileWriter {

  private Graph graph;
  private Map<PCollectionImpl<?>, Set<Target>> outputTargets;
  private List<List<Vertex>> components;

  public DotfileWriterGraph(Graph graph, Map<PCollectionImpl<?>, Set<Target>> outputTargets,
      List<List<Vertex>> components) {
    super();
    this.graph = graph;
    this.outputTargets = outputTargets;
    this.components = components;
  }

  private String formatVertex(Vertex v) {
    return formatPCollection(v.getPCollection());
  }

  private String formatNodePaths(Set<NodePath> nodePaths) {
    ArrayList<String> path = new ArrayList<String>();
    for (NodePath np : nodePaths) {
      path.add(Joiner.on(",  \\l").join(np) + " \\l");
    }
    return format("%s", Joiner.on(" | \\n").join(path));
  }

  private void link(Edge e) {
    edgeBuilder.append(String.format("%s -> %s [label=\"%s\", labeljust=r, color=\"%s\"];\n", getPCollectionID(e.getHead()
        .getPCollection()), getPCollectionID(e.getTail().getPCollection()), formatNodePaths(e.getNodePaths()), "black"));
  }

  private void link(Source source, Vertex v, String color) {
    link(source, v.getPCollection(), color);
  }

  private void link(Vertex v, Target target, String color) {
    link(v.getPCollection(), target, color);
  }

  private class ComponentContentBuilder {

    private Map<List<Vertex>, StringBuilder> contentBuilderMap = Maps.newHashMap();
    private StringBuilder topContentBuilder;

    public ComponentContentBuilder(StringBuilder contentBuilder, List<List<Vertex>> components) {
      this.topContentBuilder = contentBuilder;
      if (!CollectionUtils.isEmpty(components)) {
        for (List<Vertex> vl : components) {
          contentBuilderMap.put(vl, new StringBuilder());
        }
      }
    }

    private StringBuilder getContentBuilder(Vertex v) {
      for (Entry<List<Vertex>, StringBuilder> entry : contentBuilderMap.entrySet()) {
        if (entry.getKey().contains(v)) {
          return entry.getValue();
        }
      }
      return topContentBuilder;
    }

    public void append(Vertex v) {
      this.getContentBuilder(v).append(formatVertex(v));
    }

    public StringBuilder build() {
      int index = 0;
      for (Entry<List<Vertex>, StringBuilder> entry : contentBuilderMap.entrySet()) {
        topContentBuilder.append("subgraph \"cluster-component" + index + "\" {\n");
        topContentBuilder.append(format(
            "   label=\"Component%s\"; fontsize=14; graph[style=dotted]; fontcolor=red color=red; \n", index));
        topContentBuilder.append(entry.getValue());
        topContentBuilder.append("}\n");
        index++;
      }
      return topContentBuilder;
    }
  }

  @Override
  protected void doGetLegend(StringBuilder lsb) {
    lsb.append("   \"Folder\"  [label=\"Folder Name\", fontsize=10, shape=folder, color=darkGreen]\n")
        .append("   \"Vertex1\"  [label=\"{Vertex Name | Vertex PCollection | PType }\", fontsize=10, shape=record]\n")
        .append("   subgraph \"cluster-component-legend\" {\n")
        .append("         label=\"Component1\" fontsize=14 graph[style=dotted] fontcolor=red color=red\n")
        .append(
            "      \"Vertex2\"  [label=\"{Vertex Name | Vertex PCollection | PType }\", fontsize=10, shape=record]\n")
        .append("   }\n").append("   \"Vertex1\" -> \"Vertex2\" [label=\"Path List\", fontsize=10];\n");
  }

  @Override
  public void doBuildDiagram() {

    ComponentContentBuilder componentContentBuilder = new ComponentContentBuilder(contentBuilder, components);

    for (Vertex v : graph) {
      componentContentBuilder.append(v);

      Source source = v.getSource();
      if (source != null) {
        formatSource(source, DEFAULT_FOLDER_COLOR);
        link(source, v, DEFAULT_FOLDER_COLOR);
      }

      if (v.isOutput() && outputTargets != null) {
        for (Target target2 : outputTargets.get(v.getPCollection())) {
          formatTarget(target2, DEFAULT_FOLDER_COLOR);
          link(v, target2, DEFAULT_FOLDER_COLOR);
        }
      }
    }

    contentBuilder = componentContentBuilder.build();

    for (Edge e : graph.getAllEdges()) {
      link(e);
    }
  }
}
