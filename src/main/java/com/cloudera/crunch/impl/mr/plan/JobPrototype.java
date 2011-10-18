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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.Target;
import com.cloudera.crunch.impl.mr.collect.DoTableImpl;
import com.cloudera.crunch.impl.mr.collect.PCollectionImpl;
import com.cloudera.crunch.impl.mr.collect.PGroupedTableImpl;
import com.cloudera.crunch.impl.mr.exec.CrunchJob;
import com.cloudera.crunch.impl.mr.run.CrunchInputFormat;
import com.cloudera.crunch.impl.mr.run.CrunchMapper;
import com.cloudera.crunch.impl.mr.run.CrunchReducer;
import com.cloudera.crunch.impl.mr.run.NodeContext;
import com.cloudera.crunch.impl.mr.run.RTNodeSerializer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class JobPrototype {

  public static JobPrototype createMapReduceJob(PGroupedTableImpl group,
      Set<NodePath> inputs, Path workingPath) {
    return new JobPrototype(inputs, group, workingPath);
  }

  public static JobPrototype createMapOnlyJob(
      HashMultimap<Target, NodePath> mapNodePaths, Path workingPath) {
    return new JobPrototype(mapNodePaths, workingPath);
  }

  private final Set<NodePath> mapNodePaths;
  private final PGroupedTableImpl group;
  private final Set<JobPrototype> dependencies = Sets.newHashSet();
  private final Map<PCollectionImpl, DoNode> nodes = Maps.newHashMap();
  private final Path workingPath;
  
  private HashMultimap<Target, NodePath> targetsToNodePaths;
  private DoTableImpl combineFnTable;

  private CrunchJob job;

  private JobPrototype(Set<NodePath> inputs, PGroupedTableImpl group,
      Path workingPath) {
    this.mapNodePaths = ImmutableSet.copyOf(inputs);
    this.group = group;
    this.workingPath = workingPath;
    this.targetsToNodePaths = null;
  }

  private JobPrototype(HashMultimap<Target, NodePath> outputPaths, Path workingPath) {
    this.group = null;
    this.mapNodePaths = null;
    this.workingPath = workingPath;
    this.targetsToNodePaths = outputPaths;
  }

  public void addReducePaths(HashMultimap<Target, NodePath> outputPaths) {
    if (group == null) {
      throw new IllegalStateException(
          "Cannot add a reduce phase to a map-only job");
    }
    this.targetsToNodePaths = outputPaths;
  }

  public void addDependency(JobPrototype dependency) {
    this.dependencies.add(dependency);
  }

  public CrunchJob getCrunchJob(Class<?> jarClass, Configuration conf) throws IOException {
    if (job == null) {
      job = build(jarClass, conf);
      for (JobPrototype proto : dependencies) {
        job.addDependingJob(proto.getCrunchJob(jarClass, conf));
      }
    }
    return job;
  }

  private CrunchJob build(Class<?> jarClass, Configuration conf) throws IOException {
    Job job = new Job(conf);
    conf = job.getConfiguration();
    job.setJarByClass(jarClass);
    
    Set<DoNode> outputNodes = Sets.newHashSet();
    Set<Target> targets = targetsToNodePaths.keySet();
    MSCROutputHandler outputHandler = new MSCROutputHandler(job, workingPath,
        group == null);
    for (Target target : targets) {
      DoNode node = null;
      for (NodePath nodePath : targetsToNodePaths.get(target)) {
        if (node == null) {
          PCollectionImpl collect = nodePath.tail();
          node = DoNode.createOutputNode(target.toString(), collect.getPType());
          outputHandler.configureNode(node, target);
        }
        outputNodes.add(walkPath(nodePath.descendingIterator(), node));
      }
    }

    job.setMapperClass(CrunchMapper.class);
    List<DoNode> inputNodes;
    DoNode reduceNode = null;
    RTNodeSerializer serializer = new RTNodeSerializer();
    if (group != null) {
      job.setReducerClass(CrunchReducer.class);
      List<DoNode> reduceNodes = Lists.newArrayList(outputNodes);
      reduceNode = reduceNodes.get(0);
      serializer.serialize(reduceNodes, conf, NodeContext.REDUCE);

      group.configureShuffle(job);

      DoNode mapOutputNode = group.getGroupingNode();
      if (reduceNodes.size() == 1 && combineFnTable != null) {
        // Handle the combiner case
        DoNode mapSideCombineNode = combineFnTable.createDoNode();
        mapSideCombineNode.addChild(mapOutputNode);
        mapOutputNode = mapSideCombineNode;
      }
      
      Set<DoNode> mapNodes = Sets.newHashSet();
      for (NodePath nodePath : mapNodePaths) {
        // Advance these one step, since we've already configured
        // the grouping node, and the PGroupedTableImpl is the tail
        // of the NodePath.
        Iterator<PCollectionImpl> iter = nodePath.descendingIterator();
        iter.next();
        mapNodes.add(walkPath(iter, mapOutputNode));
      }
      inputNodes = Lists.newArrayList(mapNodes);
      serializer.serialize(inputNodes, conf, NodeContext.MAP);
    } else { // No grouping
      job.setNumReduceTasks(0);
      inputNodes = Lists.newArrayList(outputNodes);
      serializer.serialize(inputNodes, conf, NodeContext.MAP);
    }

    if (inputNodes.size() == 1) {
      DoNode inputNode = inputNodes.get(0);
      inputNode.getSource().configureSource(job, -1);
    } else {
      for (int i = 0; i < inputNodes.size(); i++) {
        DoNode inputNode = inputNodes.get(i);
        inputNode.getSource().configureSource(job, i);
      }
      job.setInputFormatClass(CrunchInputFormat.class);
    }
    job.setJobName(createJobName(inputNodes, reduceNode));
    
    return new CrunchJob(job, workingPath, outputHandler);
  }

  private String createJobName(List<DoNode> mapNodes, DoNode reduceNode) {
    JobNameBuilder builder = new JobNameBuilder();
    builder.visit(mapNodes);
    if (reduceNode != null) {
      builder.visit(reduceNode);
    }
    return builder.build();
  }
  
  private DoNode walkPath(Iterator<PCollectionImpl> iter, DoNode working) {
    while (iter.hasNext()) {
      PCollectionImpl collect = iter.next();
      if (combineFnTable != null &&
          !(collect instanceof PGroupedTableImpl)) {
        combineFnTable = null;
      } else if (collect instanceof DoTableImpl &&
          ((DoTableImpl) collect).hasCombineFn()) {
        combineFnTable = (DoTableImpl) collect;
      }
      if (!nodes.containsKey(collect)) {
        nodes.put(collect, collect.createDoNode());
      }
      DoNode parent = nodes.get(collect);
      parent.addChild(working);
      working = parent;
    }
    return working;
  }
}
