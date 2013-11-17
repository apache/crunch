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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.crunch.Pair;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 */
class Graph implements Iterable<Vertex> {

  private final Map<PCollectionImpl, Vertex> vertices;
  private final Map<Pair<Vertex, Vertex>, Edge> edges;  
  private final Map<Vertex, List<Vertex>> dependencies;
  
  public Graph() {
    this.vertices = Maps.newHashMap();
    this.edges = Maps.newHashMap();
    this.dependencies = Maps.newHashMap();
  }
  
  public Vertex getVertexAt(PCollectionImpl impl) {
    return vertices.get(impl);
  }
  
  public Vertex addVertex(PCollectionImpl impl, boolean output) {
    if (vertices.containsKey(impl)) {
      Vertex v = vertices.get(impl);
      if (output) {
        v.setOutput();
      }
      return v;
    }
    Vertex v = new Vertex(impl);
    vertices.put(impl, v);
    if (output) {
      v.setOutput();
    }
    return v;
  }
  
  public Edge getEdge(Vertex head, Vertex tail) {
    Pair<Vertex, Vertex> p = Pair.of(head, tail);
    if (edges.containsKey(p)) {
      return edges.get(p);
    }
    
    Edge e = new Edge(head, tail);
    edges.put(p, e);
    tail.addIncoming(e);
    head.addOutgoing(e);
    return e;
  }
  
  @Override
  public Iterator<Vertex> iterator() {
    return Sets.newHashSet(vertices.values()).iterator();
  }

  public Set<Edge> getAllEdges() {
    return Sets.newHashSet(edges.values());
  }
  
  public void markDependency(Vertex child, Vertex parent) {
    List<Vertex> parents = dependencies.get(child);
    if (parents == null) {
      parents = Lists.newArrayList();
      dependencies.put(child, parents);
    }
    parents.add(parent);
  }
  
  public List<Vertex> getParents(Vertex child) {
    if (dependencies.containsKey(child)) {
      return dependencies.get(child);
    }
    return ImmutableList.of();
  }
  
  public List<List<Vertex>> connectedComponents() {
    List<List<Vertex>> components = Lists.newArrayList();
    Set<Vertex> unassigned = Sets.newHashSet(vertices.values());
    while (!unassigned.isEmpty()) {
      Vertex base = unassigned.iterator().next();
      List<Vertex> component = Lists.newArrayList();
      component.add(base);
      unassigned.remove(base);
      Set<Vertex> working = Sets.newHashSet(base.getAllNeighbors());
      while (!working.isEmpty()) {
        Vertex n = working.iterator().next();
        working.remove(n);
        if (unassigned.contains(n)) {
          component.add(n);
          unassigned.remove(n);
          for (Vertex n2 : n.getAllNeighbors()) {
            if (unassigned.contains(n2)) {
              working.add(n2);
            }
          }
        }
      }
      components.add(component);
    }
    
    return components;
  }  
}
