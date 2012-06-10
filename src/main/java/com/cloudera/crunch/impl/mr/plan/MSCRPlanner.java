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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.crunch.Source;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.Target;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.impl.mr.collect.DoCollectionImpl;
import com.cloudera.crunch.impl.mr.collect.DoTableImpl;
import com.cloudera.crunch.impl.mr.collect.InputCollection;
import com.cloudera.crunch.impl.mr.collect.PCollectionImpl;
import com.cloudera.crunch.impl.mr.collect.PGroupedTableImpl;
import com.cloudera.crunch.impl.mr.collect.UnionCollection;
import com.cloudera.crunch.impl.mr.exec.MRExecutor;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MSCRPlanner {

  // Used to ensure that we always build pipelines starting from the deepest outputs, which
  // helps ensure that we handle intermediate outputs correctly.
  private static final Comparator<PCollectionImpl> DEPTH_COMPARATOR = new Comparator<PCollectionImpl>() {
    @Override
    public int compare(PCollectionImpl left, PCollectionImpl right) {
      int cmp = right.getDepth() - left.getDepth();   
      if (cmp == 0){
          // Ensure we don't throw away two output collections at the same depth.
          // Using the collection name would be nicer here, but names aren't necessarily unique
          cmp = new Integer(right.hashCode()).compareTo(left.hashCode());
      }
      return cmp;
    }
  };
  
  private final MRPipeline pipeline;
  private final Map<PCollectionImpl, Set<Target>> outputs;

  public MSCRPlanner(MRPipeline pipeline,
      Map<PCollectionImpl, Set<Target>> outputs) {
    this.pipeline = pipeline;
    this.outputs = new TreeMap<PCollectionImpl, Set<Target>>(DEPTH_COMPARATOR);
    this.outputs.putAll(outputs);
  }

  public MRExecutor plan(Class<?> jarClass, Configuration conf)
      throws IOException {
    // Constructs all of the node paths, which either start w/an input
    // or a GBK and terminate in an output collection of any type.
    NodeVisitor visitor = new NodeVisitor();
    for (PCollectionImpl output : outputs.keySet()) {
      visitor.visitOutput(output);
    }

    // Pull out the node paths.
    Map<PCollectionImpl, Set<NodePath>> nodePaths = visitor.getNodePaths();

    // Keeps track of the dependencies from collections -> jobs and then
    // between different jobs.
    Map<PCollectionImpl, JobPrototype> assignments = Maps.newHashMap();
    Map<PCollectionImpl, Set<JobPrototype>> jobDependencies =
        new HashMap<PCollectionImpl, Set<JobPrototype>>();

    // Find the set of GBKs that DO NOT depend on any other GBK.
    Set<PGroupedTableImpl> workingGroupings = null;
    while (!(workingGroupings = getWorkingGroupings(nodePaths)).isEmpty()) {

      for (PGroupedTableImpl grouping : workingGroupings) {
        Set<NodePath> mapInputPaths = nodePaths.get(grouping);
        JobPrototype proto = JobPrototype.createMapReduceJob(grouping,
            mapInputPaths, pipeline.createTempPath());
        assignments.put(grouping, proto);
        if (jobDependencies.containsKey(grouping)) {
          for (JobPrototype dependency : jobDependencies.get(grouping)) {
            proto.addDependency(dependency);
          }
        }
      }

      Map<PGroupedTableImpl, Set<NodePath>> dependencyPaths = getDependencyPaths(
          workingGroupings, nodePaths);
      for (Map.Entry<PGroupedTableImpl, Set<NodePath>> entry : dependencyPaths.entrySet()) {
        PGroupedTableImpl grouping = entry.getKey();
        Set<NodePath> currentNodePaths = entry.getValue();

        JobPrototype proto = assignments.get(grouping);
        Set<NodePath> gbkPaths = Sets.newHashSet();
        for (NodePath nodePath : currentNodePaths) {
          PCollectionImpl tail = nodePath.tail();
          if (tail instanceof PGroupedTableImpl) {
            gbkPaths.add(nodePath);
            if (!jobDependencies.containsKey(tail)) {
              jobDependencies.put(tail, Sets.<JobPrototype>newHashSet());
            }
            jobDependencies.get(tail).add(proto);
          }
        }

        if (!gbkPaths.isEmpty()) {
          handleGroupingDependencies(gbkPaths, currentNodePaths);
        }

        // At this point, all of the dependencies for the working groups will be
        // file outputs, and so we can add them all to the JobPrototype-- we now have
        // a complete job.
        HashMultimap<Target, NodePath> reduceOutputs = HashMultimap.create();
        for (NodePath nodePath : currentNodePaths) {
          assignments.put(nodePath.tail(), proto);
          for (Target target : outputs.get(nodePath.tail())) {
            reduceOutputs.put(target, nodePath);
          }
        }
        proto.addReducePaths(reduceOutputs);

        // We've processed this GBK-- remove it from the set of nodePaths we
        // need to process in the next step.
        nodePaths.remove(grouping);
      }
    }

    // Process any map-only jobs that are remaining.
    if (!nodePaths.isEmpty()) {
      for (Map.Entry<PCollectionImpl, Set<NodePath>> entry : nodePaths
          .entrySet()) {
        PCollectionImpl collect = entry.getKey();
        if (!assignments.containsKey(collect)) {
          HashMultimap<Target, NodePath> mapOutputs = HashMultimap.create();
          for (NodePath nodePath : entry.getValue()) {
            for (Target target : outputs.get(nodePath.tail())) {
              mapOutputs.put(target, nodePath);
            }
          }
          JobPrototype proto = JobPrototype.createMapOnlyJob(mapOutputs,
              pipeline.createTempPath());
          
          if (jobDependencies.containsKey(collect)) {
            for (JobPrototype dependency : jobDependencies.get(collect)) {
              proto.addDependency(dependency);
            }
          }
          assignments.put(collect, proto);
        }
      }
    }

    MRExecutor exec = new MRExecutor(jarClass);
    for (JobPrototype proto : Sets.newHashSet(assignments.values())) {
      exec.addJob(proto.getCrunchJob(jarClass, conf, pipeline));
    }
    return exec;
  }

  private Map<PGroupedTableImpl, Set<NodePath>> getDependencyPaths(
      Set<PGroupedTableImpl> workingGroupings,
      Map<PCollectionImpl, Set<NodePath>> nodePaths) {
    Map<PGroupedTableImpl, Set<NodePath>> dependencyPaths = Maps.newHashMap();
    for (PGroupedTableImpl grouping : workingGroupings) {
      dependencyPaths.put(grouping, Sets.<NodePath> newHashSet());
    }

    // Find the targets that depend on one of the elements of the current
    // working group.
    for (PCollectionImpl target : nodePaths.keySet()) {
      if (!workingGroupings.contains(target)) {
        for (NodePath nodePath : nodePaths.get(target)) {
          if (workingGroupings.contains(nodePath.head())) {
            dependencyPaths.get(nodePath.head()).add(nodePath);
          }
        }
      }
    }
    return dependencyPaths;
  }

  private int getSplitIndex(Set<NodePath> currentNodePaths) {
    List<Iterator<PCollectionImpl>> iters = Lists.newArrayList();
    for (NodePath nodePath : currentNodePaths) {
      Iterator<PCollectionImpl> iter = nodePath.iterator();
      iter.next(); // prime this past the initial NGroupedTableImpl
      iters.add(iter);
    }

    // Find the lowest point w/the lowest cost to be the split point for
    // all of the dependent paths.
    boolean end = false;
    int splitIndex = -1;
    while (!end) {
      splitIndex++;
      PCollectionImpl current = null;
      for (Iterator<PCollectionImpl> iter : iters) {
        if (iter.hasNext()) {
          PCollectionImpl next = iter.next();
          if (next instanceof PGroupedTableImpl) {
            end = true;
            break;
          } else if (current == null) {
            current = next;
          } else if (current != next) {
            end = true;
            break;
          }
        } else {
          end = true;
          break;
        }
      }
    }
    // TODO: Add costing calcs here.
    return splitIndex;
  }

  private void handleGroupingDependencies(Set<NodePath> gbkPaths,
      Set<NodePath> currentNodePaths) throws IOException {
    int splitIndex = getSplitIndex(currentNodePaths);
    PCollectionImpl splitTarget = currentNodePaths.iterator().next()
        .get(splitIndex);
    if (!outputs.containsKey(splitTarget)) {
      outputs.put(splitTarget, Sets.<Target>newHashSet());
    }
    
    SourceTarget srcTarget = null;
    Target targetToReplace = null;
    for (Target t : outputs.get(splitTarget)) {
      if (t instanceof SourceTarget) {
        srcTarget = (SourceTarget) t;
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

    PCollectionImpl inputNode = (PCollectionImpl) pipeline.read(srcTarget);
    Set<NodePath> nextNodePaths = Sets.newHashSet();
    for (NodePath nodePath : currentNodePaths) {
      if (gbkPaths.contains(nodePath)) {
    	nextNodePaths.add(nodePath.splitAt(splitIndex, inputNode));
      } else {
    	nextNodePaths.add(nodePath);
      }
    }
    currentNodePaths.clear();
    currentNodePaths.addAll(nextNodePaths);
  }

  private Set<PGroupedTableImpl> getWorkingGroupings(
      Map<PCollectionImpl, Set<NodePath>> nodePaths) {
    Set<PGroupedTableImpl> gbks = Sets.newHashSet();
    for (PCollectionImpl target : nodePaths.keySet()) {
      if (target instanceof PGroupedTableImpl) {
        boolean hasGBKDependency = false;
        for (NodePath nodePath : nodePaths.get(target)) {
          if (nodePath.head() instanceof PGroupedTableImpl) {
            hasGBKDependency = true;
            break;
          }
        }
        if (!hasGBKDependency) {
          gbks.add((PGroupedTableImpl) target);
        }
      }
    }
    return gbks;
  }

  private static class NodeVisitor implements PCollectionImpl.Visitor {

    private final Map<PCollectionImpl, Set<NodePath>> nodePaths;
    private final Map<PCollectionImpl, Source> inputs;
    private PCollectionImpl workingNode;
    private NodePath workingPath;

    public NodeVisitor() {
      this.nodePaths = new HashMap<PCollectionImpl, Set<NodePath>>();
      this.inputs = new HashMap<PCollectionImpl, Source>();
    }

    public Map<PCollectionImpl, Set<NodePath>> getNodePaths() {
      return nodePaths;
    }

    public void visitOutput(PCollectionImpl output) {
      nodePaths.put(output, Sets.<NodePath> newHashSet());
      workingNode = output;
      workingPath = new NodePath();
      output.accept(this);
    }

    @Override
    public void visitInputCollection(InputCollection<?> collection) {
      workingPath.close(collection);
      inputs.put(collection, collection.getSource());
      nodePaths.get(workingNode).add(workingPath);
    }

    @Override
    public void visitUnionCollection(UnionCollection<?> collection) {
      PCollectionImpl baseNode = workingNode;
      NodePath basePath = workingPath;
      for (PCollectionImpl parent : collection.getParents()) {
        workingPath = new NodePath(basePath);
        workingNode = baseNode;
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
      workingPath.close(collection);
      nodePaths.get(workingNode).add(workingPath);
      workingNode = collection;
      nodePaths.put(workingNode, Sets.<NodePath> newHashSet());
      workingPath = new NodePath(collection);
      processParent(collection.getOnlyParent());
    }

    private void processParent(PCollectionImpl parent) {
      if (!nodePaths.containsKey(parent)) {
        parent.accept(this);
      } else {
        workingPath.close(parent);
        nodePaths.get(workingNode).add(workingPath);
      }
    }
  }
}
