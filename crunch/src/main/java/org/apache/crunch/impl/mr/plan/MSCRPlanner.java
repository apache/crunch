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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.crunch.Source;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.collect.DoCollectionImpl;
import org.apache.crunch.impl.mr.collect.DoTableImpl;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;
import org.apache.crunch.impl.mr.collect.UnionCollection;
import org.apache.crunch.impl.mr.exec.MRExecutor;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MSCRPlanner {

  // Used to ensure that we always build pipelines starting from the deepest
  // outputs, which
  // helps ensure that we handle intermediate outputs correctly.
  private static final Comparator<PCollectionImpl<?>> DEPTH_COMPARATOR = new Comparator<PCollectionImpl<?>>() {
    @Override
    public int compare(PCollectionImpl<?> left, PCollectionImpl<?> right) {
      int cmp = right.getDepth() - left.getDepth();
      if (cmp == 0) {
        // Ensure we don't throw away two output collections at the same depth.
        // Using the collection name would be nicer here, but names aren't
        // necessarily unique
        cmp = new Integer(right.hashCode()).compareTo(left.hashCode());
      }
      return cmp;
    }
  };

  private final MRPipeline pipeline;
  private final Map<PCollectionImpl<?>, Set<Target>> outputs;

  public MSCRPlanner(MRPipeline pipeline, Map<PCollectionImpl<?>, Set<Target>> outputs) {
    this.pipeline = pipeline;
    this.outputs = new TreeMap<PCollectionImpl<?>, Set<Target>>(DEPTH_COMPARATOR);
    this.outputs.putAll(outputs);
  }

  public MRExecutor plan(Class<?> jarClass, Configuration conf) throws IOException {
    // Walk the current plan tree and build a graph in which the vertices are
    // sources, targets, and GBK operations.
    GraphBuilder graphBuilder = new GraphBuilder();
    for (PCollectionImpl<?> output : outputs.keySet()) {
      graphBuilder.visitOutput(output);
    }
    Graph baseGraph = graphBuilder.getGraph();
    
    // Create a new graph that splits up up dependent GBK nodes.
    Graph graph = prepareFinalGraph(baseGraph);
    
    // Break the graph up into connected components.
    List<List<Vertex>> components = graph.connectedComponents();
    
    // For each component, we will create one or more job prototypes,
    // depending on its profile.
    // For dependency handling, we only need to care about which
    // job prototype a particular GBK is assigned to.
    Map<Vertex, JobPrototype> assignments = Maps.newHashMap();
    for (List<Vertex> component : components) {
      assignments.putAll(constructJobPrototypes(component));
    }
    
    // Add in the job dependency information here.
    for (Map.Entry<Vertex, JobPrototype> e : assignments.entrySet()) {
      JobPrototype current = e.getValue();
      List<Vertex> parents = graph.getParents(e.getKey());
      for (Vertex parent : parents) {
        current.addDependency(assignments.get(parent));
      }
    }
    
    // Finally, construct the jobs from the prototypes and return.
    MRExecutor exec = new MRExecutor(jarClass);
    for (JobPrototype proto : Sets.newHashSet(assignments.values())) {
      exec.addJob(proto.getCrunchJob(jarClass, conf, pipeline));
    }
    return exec;
  }
  
  private Graph prepareFinalGraph(Graph baseGraph) {
    Graph graph = new Graph();
    
    for (Vertex baseVertex : baseGraph) {
      // Add all of the vertices in the base graph, but no edges (yet).
      graph.addVertex(baseVertex.getPCollection());
    }
    
    for (Edge e : baseGraph.getAllEdges()) {
      // Add back all of the edges where neither vertex is a GBK.
      if (!e.getHead().isGBK() && !e.getTail().isGBK()) {
        Vertex head = graph.getVertexAt(e.getHead().getPCollection());
        Vertex tail = graph.getVertexAt(e.getTail().getPCollection());
        graph.getEdge(head, tail).addAllNodePaths(e.getNodePaths());
      }
    }
    
    for (Vertex baseVertex : baseGraph) {
      if (baseVertex.isGBK()) {
        Vertex vertex = graph.getVertexAt(baseVertex.getPCollection());
        for (Edge e : baseVertex.getIncomingEdges()) {
          if (!e.getHead().isGBK()) {
            Vertex newHead = graph.getVertexAt(e.getHead().getPCollection());
            graph.getEdge(newHead, vertex).addAllNodePaths(e.getNodePaths());
          }
        }
        for (Edge e : baseVertex.getOutgoingEdges()) {
          if (!e.getTail().isGBK()) {
            Vertex newTail = graph.getVertexAt(e.getTail().getPCollection());
            graph.getEdge(vertex, newTail).addAllNodePaths(e.getNodePaths());
          } else {
            
            // Execute an Edge split
            Vertex newGraphTail = graph.getVertexAt(e.getTail().getPCollection());
            PCollectionImpl split = e.getSplit();
            InputCollection<?> inputNode = handleSplitTarget(split);
            Vertex splitTail = graph.addVertex(split);
            Vertex splitHead = graph.addVertex(inputNode);
            
            // Divide up the node paths in the edge between the two GBK nodes so
            // that each node is either owned by GBK1 -> newTail or newHead -> GBK2.
            for (NodePath path : e.getNodePaths()) {
              NodePath headPath = path.splitAt(split, splitHead.getPCollection());
              graph.getEdge(vertex, splitTail).addNodePath(headPath);
              graph.getEdge(splitHead, newGraphTail).addNodePath(path);
            }
            
            // Note the dependency between the vertices in the graph.
            graph.markDependency(splitHead, splitTail);
          }
        }
      }
    }
    
    return graph;
  }
  
  private Map<Vertex, JobPrototype> constructJobPrototypes(List<Vertex> component) {
    Map<Vertex, JobPrototype> assignment = Maps.newHashMap();
    List<Vertex> gbks = Lists.newArrayList();
    for (Vertex v : component) {
      if (v.isGBK()) {
        gbks.add(v);
      }
    }

    if (gbks.isEmpty()) {
      HashMultimap<Target, NodePath> outputPaths = HashMultimap.create();
      for (Vertex v : component) {
        if (v.isInput()) {
          for (Edge e : v.getOutgoingEdges()) {
            for (NodePath nodePath : e.getNodePaths()) {
              PCollectionImpl target = nodePath.tail();
              for (Target t : outputs.get(target)) {
                outputPaths.put(t, nodePath);
              }
            }
          }
        }
      }
      if (outputPaths.isEmpty()) {
        throw new IllegalStateException("No outputs?");
      }
      JobPrototype prototype = JobPrototype.createMapOnlyJob(
          outputPaths, pipeline.createTempPath()); 
      for (Vertex v : component) {
        assignment.put(v, prototype);
      }
    } else {
      for (Vertex g : gbks) {
        Set<NodePath> inputs = Sets.newHashSet();
        for (Edge e : g.getIncomingEdges()) {
          inputs.addAll(e.getNodePaths());
        }
        JobPrototype prototype = JobPrototype.createMapReduceJob(
            (PGroupedTableImpl) g.getPCollection(), inputs, pipeline.createTempPath());
        assignment.put(g, prototype);
        for (Edge e : g.getIncomingEdges()) {
          assignment.put(e.getHead(), prototype);
        }
        HashMultimap<Target, NodePath> outputPaths = HashMultimap.create();
        for (Edge e : g.getOutgoingEdges()) {
          Vertex output = e.getTail();
          for (Target t : outputs.get(output.getPCollection())) {
            outputPaths.putAll(t, e.getNodePaths());
          }
          assignment.put(output, prototype);
        }
        prototype.addReducePaths(outputPaths);
      }
    }
    
    return assignment;
  }
  
  private InputCollection<?> handleSplitTarget(PCollectionImpl<?> splitTarget) {
    if (!outputs.containsKey(splitTarget)) {
      outputs.put(splitTarget, Sets.<Target> newHashSet());
    }

    SourceTarget srcTarget = null;
    Target targetToReplace = null;
    for (Target t : outputs.get(splitTarget)) {
      if (t instanceof SourceTarget) {
        srcTarget = (SourceTarget<?>) t;
        break;
      } else {
        srcTarget = t.asSourceTarget(splitTarget.getPType());
        if (srcTarget != null) {
          targetToReplace = t;
          break;
        }
      }
    }
    if (targetToReplace != null) {
      outputs.get(splitTarget).remove(targetToReplace);
    } else if (srcTarget == null) {
      srcTarget = pipeline.createIntermediateOutput(splitTarget.getPType());
    }
    outputs.get(splitTarget).add(srcTarget);
    splitTarget.materializeAt(srcTarget);

    return (InputCollection<?>) pipeline.read(srcTarget);
  }  
}
