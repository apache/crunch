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

import org.apache.crunch.impl.mr.collect.DoCollectionImpl;
import org.apache.crunch.impl.mr.collect.DoTableImpl;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;
import org.apache.crunch.impl.mr.collect.UnionCollection;

/**
 *
 */
public class GraphBuilder implements PCollectionImpl.Visitor {

  private Graph graph = new Graph();
  private Vertex workingVertex;
  private NodePath workingPath;
  
  public Graph getGraph() {
    return graph;
  }
  
  public void visitOutput(PCollectionImpl<?> output) {
    workingVertex = graph.addVertex(output, true);
    workingPath = new NodePath();
    output.accept(this);
  }
  
  @Override
  public void visitInputCollection(InputCollection<?> collection) {
    Vertex v = graph.addVertex(collection, false);
    graph.getEdge(v, workingVertex).addNodePath(workingPath.close(collection));
  }

  @Override
  public void visitUnionCollection(UnionCollection<?> collection) {
    Vertex baseVertex = workingVertex;
    NodePath basePath = workingPath;
    for (PCollectionImpl<?> parent : collection.getParents()) {
      workingPath = new NodePath(basePath);
      workingVertex = baseVertex;
      processParent(parent);
    }
  }

  @Override
  public void visitDoFnCollection(DoCollectionImpl<?> collection) {
    workingPath.push(collection);
    processParent(collection.getOnlyParent());
  }

  @Override
  public void visitDoTable(DoTableImpl<?, ?> collection) {
    workingPath.push(collection);
    processParent(collection.getOnlyParent());
  }

  @Override
  public void visitGroupedTable(PGroupedTableImpl<?, ?> collection) {
    Vertex v = graph.addVertex(collection, false);
    graph.getEdge(v, workingVertex).addNodePath(workingPath.close(collection));
    workingVertex = v;
    workingPath = new NodePath(collection);
    processParent(collection.getOnlyParent());
  }
  
  private void processParent(PCollectionImpl<?> parent) {
    Vertex v = graph.getVertexAt(parent);
    if (v == null) {
      parent.accept(this);
    } else {
      graph.getEdge(v, workingVertex).addNodePath(workingPath.close(parent));
    }
  }
}
