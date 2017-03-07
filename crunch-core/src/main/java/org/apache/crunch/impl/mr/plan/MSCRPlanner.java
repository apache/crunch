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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.crunch.PipelineCallable;
import org.apache.crunch.Source;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;
import org.apache.crunch.impl.mr.exec.MRExecutor;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class MSCRPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(MSCRPlanner.class);

  private final MRPipeline pipeline;
  private final Map<PCollectionImpl<?>, Set<Target>> outputs;
  private final Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize;
  private final Set<Target> appendedTargets;
  private final Map<PipelineCallable<?>, Set<Target>> pipelineCallables;
  private int lastJobID = 0;

  public MSCRPlanner(MRPipeline pipeline, Map<PCollectionImpl<?>, Set<Target>> outputs,
      Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize,
      Set<Target> appendedTargets,
      Map<PipelineCallable<?>, Set<Target>> pipelineCallables) {
    this.pipeline = pipeline;
    this.outputs = new TreeMap<PCollectionImpl<?>, Set<Target>>(DEPTH_COMPARATOR);
    this.outputs.putAll(outputs);
    this.toMaterialize = toMaterialize;
    this.appendedTargets = appendedTargets;
    this.pipelineCallables = pipelineCallables;
  }

  // Used to ensure that we always build pipelines starting from the deepest
  // outputs, which helps ensure that we handle intermediate outputs correctly.
  static final Comparator<PCollectionImpl<?>> DEPTH_COMPARATOR = new Comparator<PCollectionImpl<?>>() {
    @Override
    public int compare(PCollectionImpl<?> left, PCollectionImpl<?> right) {
      int cmp = right.getDepth() - left.getDepth();
      if (cmp == 0) {
        // Ensure we don't throw away two output collections at the same depth.
        // Using the collection name would be nicer here, but names aren't
        // necessarily unique.
        cmp = new Integer(right.hashCode()).compareTo(left.hashCode());
      }
      return cmp;
    }
  };  

  public MRExecutor plan(Class<?> jarClass, Configuration conf) throws IOException {

    DotfileUtil dotfileUtil = new DotfileUtil(jarClass, conf);

    // Generate the debug lineage dotfiles (if configuration is enabled)
    dotfileUtil.buildLineageDotfile(outputs);

    Map<PCollectionImpl<?>, Set<Target>> targetDeps = Maps.newTreeMap(DEPTH_COMPARATOR);
    for (PCollectionImpl<?> pcollect : outputs.keySet()) {
      targetDeps.put(pcollect, pcollect.getTargetDependencies());
    }

    Multimap<Target, JobPrototype> assignments = HashMultimap.create();

    while (!targetDeps.isEmpty()) {
      Set<Target> allTargets = Sets.newHashSet();
      for (PCollectionImpl<?> pcollect : targetDeps.keySet()) {
        allTargets.addAll(outputs.get(pcollect));
      }
      GraphBuilder graphBuilder = new GraphBuilder();

      // Walk the current plan tree and build a graph in which the vertices are
      // sources, targets, and GBK operations.
      Set<PCollectionImpl<?>> currentStage = Sets.newHashSet();
      for (PCollectionImpl<?> output : targetDeps.keySet()) {
        Set<Target> deps = Sets.intersection(allTargets, targetDeps.get(output));
        if (deps.isEmpty()) {
          graphBuilder.visitOutput(output);
          currentStage.add(output);
        }
      }

      Graph baseGraph = graphBuilder.getGraph();
      boolean hasInputs = false;
      for (Vertex v : baseGraph) {
        if (v.isInput()) {
          hasInputs = true;
          break;
        }
      }
      if (!hasInputs) {
        LOG.warn("No input sources for pipeline, nothing to do...");
        return new MRExecutor(conf, jarClass, outputs, toMaterialize, appendedTargets, pipelineCallables);
      }

      // Create a new graph that splits up up dependent GBK nodes.
      Graph graph = prepareFinalGraph(baseGraph);

      // Break the graph up into connected components.
      List<List<Vertex>> components = graph.connectedComponents();

      // Generate the debug graph dotfiles (if configuration is enabled)
      dotfileUtil.buildBaseGraphDotfile(outputs, baseGraph);
      dotfileUtil.buildSplitGraphDotfile(outputs, graph);
      dotfileUtil.buildSplitGraphWithComponentsDotfile(outputs, graph, components);

      // For each component, we will create one or more job prototypes,
      // depending on its profile.
      // For dependency handling, we only need to care about which
      // job prototype a particular GBK is assigned to.
      Multimap<Vertex, JobPrototype> newAssignments = HashMultimap.create();
      for (List<Vertex> component : components) {
        newAssignments.putAll(constructJobPrototypes(component));
      }

      // Add in the job dependency information here.
      for (Map.Entry<Vertex, JobPrototype> e : newAssignments.entries()) {
        JobPrototype current = e.getValue();
        for (Vertex parent : graph.getParents(e.getKey())) {
          for (JobPrototype parentJobProto : newAssignments.get(parent)) {
            current.addDependency(parentJobProto);
          }
        }
      }

      ImmutableMultimap<Target, JobPrototype> previousStages = ImmutableMultimap.copyOf(assignments);
      for (Map.Entry<Vertex, JobPrototype> e : newAssignments.entries()) {
        if (e.getKey().isOutput()) {
          PCollectionImpl<?> pcollect = e.getKey().getPCollection();
          JobPrototype current = e.getValue();

          // Add in implicit dependencies via SourceTargets that are read into memory
          for (Target pt : pcollect.getTargetDependencies()) {
            for (JobPrototype parentJobProto : assignments.get(pt)) {
              current.addDependency(parentJobProto);
            }
          }

          // Add this to the set of output assignments
          for (Target t : outputs.get(pcollect)) {
            assignments.put(t, e.getValue());
          }
        } else {
          Source source = e.getKey().getSource();
          if (source != null && source instanceof Target) {
            JobPrototype current = e.getValue();
            Collection<JobPrototype> parentJobPrototypes = previousStages.get((Target) source);
            if (parentJobPrototypes != null) {
              for (JobPrototype parentJobProto : parentJobPrototypes) {
                current.addDependency(parentJobProto);
              }
            }
          }
        }
      }

      // Remove completed outputs and mark materialized output locations
      // for subsequent job processing.
      for (PCollectionImpl<?> output : currentStage) {
        if (toMaterialize.containsKey(output)) {
          MaterializableIterable mi = toMaterialize.get(output);
          if (mi.isSourceTarget()) {
            output.materializeAt((SourceTarget) mi.getSource());
          }
        }
        targetDeps.remove(output);
      }
    }

    // Finally, construct the jobs from the prototypes and return.
    MRExecutor exec = new MRExecutor(conf, jarClass, outputs, toMaterialize, appendedTargets, pipelineCallables);

    // Generate the debug Plan dotfiles
    dotfileUtil.buildPlanDotfile(exec, assignments, pipeline, lastJobID);

    for (JobPrototype proto : Sets.newHashSet(assignments.values())) {
      exec.addJob(proto.getCrunchJob(jarClass, conf, pipeline, lastJobID));
    }

    // Generate the debug RTNode dotfiles (if configuration is enabled)
    dotfileUtil.buildRTNodesDotfile(exec);

    // Attach the dotfiles to the MRExcutor context
    dotfileUtil.addDotfilesToContext(exec);

    return exec;
  }
  
  private Graph prepareFinalGraph(Graph baseGraph) {
    Graph graph = new Graph();
    
    for (Vertex baseVertex : baseGraph) {
      // Add all of the vertices in the base graph, but no edges (yet).
      graph.addVertex(baseVertex.getPCollection(), baseVertex.isOutput());
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
          if (e.getHead().isOutput()) {
            // Execute an edge split.
            Vertex splitTail = e.getHead();
            PCollectionImpl<?> split = splitTail.getPCollection();
            InputCollection<?> inputNode = handleSplitTarget(split);
            Vertex splitHead = graph.addVertex(inputNode, false);
            
            // Divide up the node paths in the edge between the two GBK nodes so
            // that each node is either owned by GBK1 -> newTail or newHead -> GBK2.
            for (NodePath path : e.getNodePaths()) {
              NodePath headPath = path.splitAt(split, splitHead.getPCollection());
              graph.getEdge(vertex, splitTail).addNodePath(headPath);
              graph.getEdge(splitHead, vertex).addNodePath(path);
            }
            
            // Note the dependency between the vertices in the graph.
            graph.markDependency(splitHead, splitTail);
          } else if (!e.getHead().isGBK()) {
            Vertex newHead = graph.getVertexAt(e.getHead().getPCollection());
            Map<NodePath, PCollectionImpl> splitPoints = e.getSplitPoints(true /* breakpoints only  */);
            if (splitPoints.isEmpty()) {
              graph.getEdge(newHead, vertex).addAllNodePaths(e.getNodePaths());
            } else {
              for (Map.Entry<NodePath, PCollectionImpl> s : splitPoints.entrySet()) {
                NodePath path = s.getKey();
                PCollectionImpl split = s.getValue();
                InputCollection<?> inputNode = handleSplitTarget(split);
                Vertex splitTail = graph.addVertex(split, true);
                Vertex splitHead = graph.addVertex(inputNode, false);
                NodePath headPath = path.splitAt(split, splitHead.getPCollection());
                graph.getEdge(newHead, splitTail).addNodePath(headPath);
                graph.getEdge(splitHead, vertex).addNodePath(path);
                // Note the dependency between the vertices in the graph.
                graph.markDependency(splitHead, splitTail);
              }
            }
          }
        }
        for (Edge e : baseVertex.getOutgoingEdges()) {
          if (!e.getTail().isGBK()) {
            Vertex newTail = graph.getVertexAt(e.getTail().getPCollection());
            graph.getEdge(vertex, newTail).addAllNodePaths(e.getNodePaths());
          } else {
            // Execute an Edge split
            Vertex newGraphTail = graph.getVertexAt(e.getTail().getPCollection());
            Map<NodePath, PCollectionImpl> splitPoints = e.getSplitPoints(false /* breakpoints only */);
            for (Map.Entry<NodePath, PCollectionImpl> s : splitPoints.entrySet()) {
              NodePath path = s.getKey();
              PCollectionImpl split = s.getValue();
              InputCollection<?> inputNode = handleSplitTarget(split);
              Vertex splitTail = graph.addVertex(split, true);
              Vertex splitHead = graph.addVertex(inputNode, false);
              NodePath headPath = path.splitAt(split, splitHead.getPCollection());
              graph.getEdge(vertex, splitTail).addNodePath(headPath);
              graph.getEdge(splitHead, newGraphTail).addNodePath(path);
              // Note the dependency between the vertices in the graph.
              graph.markDependency(splitHead, splitTail);
            }
          }
        }
      }
    }
    
    return graph;
  }
  
  private Multimap<Vertex, JobPrototype> constructJobPrototypes(List<Vertex> component) {
    Multimap<Vertex, JobPrototype> assignment = HashMultimap.create();
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
          ++lastJobID, outputPaths, pipeline.createTempPath());
      for (Vertex v : component) {
        assignment.put(v, prototype);
      }
    } else {
      Set<Edge> usedEdges = Sets.newHashSet();
      for (Vertex g : gbks) {
        Set<NodePath> inputs = Sets.newHashSet();
        HashMultimap<Target, NodePath> mapSideOutputPaths = HashMultimap.create();
        for (Edge e : g.getIncomingEdges()) {
          inputs.addAll(e.getNodePaths());
          usedEdges.add(e);
          if (e.getHead().isInput()) {
            for (Edge ep : e.getHead().getOutgoingEdges()) {
              if (ep.getTail().isOutput() && !usedEdges.contains(ep)) { // map-side output
                for (Target t : outputs.get(ep.getTail().getPCollection())) {
                  mapSideOutputPaths.putAll(t, ep.getNodePaths());
                }
                usedEdges.add(ep);
              }
            }
          }
        }
        JobPrototype prototype = JobPrototype.createMapReduceJob(
            ++lastJobID, (PGroupedTableImpl) g.getPCollection(), inputs, pipeline.createTempPath());
        prototype.addMapSideOutputs(mapSideOutputPaths);
        assignment.put(g, prototype);
        for (Edge e : g.getIncomingEdges()) {
          assignment.put(e.getHead(), prototype);
          if (e.getHead().isInput()) {
            for (Edge ep : e.getHead().getOutgoingEdges()) {
              if (ep.getTail().isOutput() && !assignment.containsKey(ep.getTail())) { // map-side output
                assignment.put(ep.getTail(), prototype);
              }
            }
          }
        }
        
        HashMultimap<Target, NodePath> outputPaths = HashMultimap.create();
        for (Edge e : g.getOutgoingEdges()) {
          Vertex output = e.getTail();
          for (Target t : outputs.get(output.getPCollection())) {
            outputPaths.putAll(t, e.getNodePaths());
          }
          assignment.put(output, prototype);
          usedEdges.add(e);
        }
        prototype.addReducePaths(outputPaths);
      }

      // Check for any un-assigned vertices, which should be map-side outputs
      // that we will need to run in a map-only job.
      HashMultimap<Target, NodePath> outputPaths = HashMultimap.create();
      Set<Vertex> orphans = Sets.newHashSet();
      for (Vertex v : component) {
        // Check if this vertex has multiple inputs but only a subset of
        // them have already been assigned
        boolean vertexHasUnassignedIncomingEdges = false;
        if (v.isOutput()) {
          for (Edge e : v.getIncomingEdges()) {
            if (!usedEdges.contains(e)) {
              vertexHasUnassignedIncomingEdges = true;
            }
          }
        }

        if (v.isOutput() && (vertexHasUnassignedIncomingEdges || !assignment.containsKey(v))) {
          orphans.add(v);
          for (Edge e : v.getIncomingEdges()) {
            if (vertexHasUnassignedIncomingEdges && usedEdges.contains(e)) {
              // We've already dealt with this incoming edge
              continue;
            }
            orphans.add(e.getHead());
            for (NodePath nodePath : e.getNodePaths()) {
              PCollectionImpl target = nodePath.tail();
              for (Target t : outputs.get(target)) {
                outputPaths.put(t, nodePath);
              }
            }
          }
        }

      }
      if (!outputPaths.isEmpty()) {
        JobPrototype prototype = JobPrototype.createMapOnlyJob(
            ++lastJobID, outputPaths, pipeline.createTempPath());
        for (Vertex orphan : orphans) {
          assignment.put(orphan, prototype);
        }
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
