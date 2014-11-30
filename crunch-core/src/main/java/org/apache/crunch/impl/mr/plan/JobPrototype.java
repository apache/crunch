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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.DoTable;
import org.apache.crunch.impl.dist.collect.MRCollection;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;
import org.apache.crunch.impl.mr.exec.CrunchJobHooks;
import org.apache.crunch.impl.mr.run.CrunchCombiner;
import org.apache.crunch.impl.mr.run.CrunchInputFormat;
import org.apache.crunch.impl.mr.run.CrunchMapper;
import org.apache.crunch.impl.mr.run.CrunchOutputFormat;
import org.apache.crunch.impl.mr.run.CrunchReducer;
import org.apache.crunch.impl.mr.run.NodeContext;
import org.apache.crunch.impl.mr.run.RTNode;
import org.apache.crunch.types.PType;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

class JobPrototype {

  public static JobPrototype createMapReduceJob(int jobID, PGroupedTableImpl<?, ?> group,
      Set<NodePath> inputs, Path workingPath) {
    return new JobPrototype(jobID, inputs, group, workingPath);
  }

  public static JobPrototype createMapOnlyJob(int jobID, HashMultimap<Target, NodePath> mapNodePaths, Path workingPath) {
    return new JobPrototype(jobID, mapNodePaths, workingPath);
  }

  private final int jobID; // TODO: maybe stageID sounds better
  private final Set<NodePath> mapNodePaths;
  private final PGroupedTableImpl<?, ?> group;
  private final Set<JobPrototype> dependencies = Sets.newHashSet();
  private final Map<PCollectionImpl<?>, DoNode> nodes = Maps.newHashMap();
  private final Path workingPath;

  private HashMultimap<Target, NodePath> mapSideNodePaths;
  private HashMultimap<Target, NodePath> targetsToNodePaths;
  private DoTable<?, ?> combineFnTable;

  private CrunchControlledJob job;

  private JobPrototype(int jobID, Set<NodePath> inputs, PGroupedTableImpl<?, ?> group, Path workingPath) {
    this.jobID = jobID;
    this.mapNodePaths = ImmutableSet.copyOf(inputs);
    this.group = group;
    this.workingPath = workingPath;
    this.targetsToNodePaths = null;
  }

  private JobPrototype(int jobID, HashMultimap<Target, NodePath> outputPaths, Path workingPath) {
    this.jobID = jobID;
    this.group = null;
    this.mapNodePaths = null;
    this.workingPath = workingPath;
    this.targetsToNodePaths = outputPaths;
  }

  public int getJobID() {
    return jobID;
  }

  public boolean isMapOnly() {
    return this.group == null;
  }

  Set<NodePath> getMapNodePaths() {
    return mapNodePaths;
  }

  HashMultimap<Target, NodePath> getMapSideNodePaths() {
    return mapSideNodePaths;
  }

  HashMultimap<Target, NodePath> getTargetsToNodePaths() {
    return targetsToNodePaths;
  }

  public void addMapSideOutputs(HashMultimap<Target, NodePath> mapSideNodePaths) {
    if (group == null) {
      throw new IllegalStateException("Cannot side-outputs to a map-only job");
    }
    this.mapSideNodePaths = mapSideNodePaths;
  }
  
  public void addReducePaths(HashMultimap<Target, NodePath> outputPaths) {
    if (group == null) {
      throw new IllegalStateException("Cannot add a reduce phase to a map-only job");
    }
    this.targetsToNodePaths = outputPaths;
  }

  public void addDependency(JobPrototype dependency) {
    this.dependencies.add(dependency);
  }

  public CrunchControlledJob getCrunchJob(
      Class<?> jarClass, Configuration conf, Pipeline pipeline, int numOfJobs) throws IOException {
    if (job == null) {
      job = build(jarClass, conf, pipeline, numOfJobs);
      for (JobPrototype proto : dependencies) {
        job.addDependingJob(proto.getCrunchJob(jarClass, conf, pipeline, numOfJobs));
      }
    }
    return job;
  }

  private CrunchControlledJob build(
      Class<?> jarClass, Configuration conf, Pipeline pipeline, int numOfJobs) throws IOException {
    Job job = new Job(conf);
    conf = job.getConfiguration();
    conf.set(PlanningParameters.CRUNCH_WORKING_DIRECTORY, workingPath.toString());
    job.setJarByClass(jarClass);

    Set<DoNode> outputNodes = Sets.newHashSet();
    Set<Target> allTargets = Sets.newHashSet();
    Path outputPath = new Path(workingPath, "output");
    MSCROutputHandler outputHandler = new MSCROutputHandler(job, outputPath, group == null);
    for (Target target : targetsToNodePaths.keySet()) {
      DoNode node = null;
      for (NodePath nodePath : targetsToNodePaths.get(target)) {
        if (node == null) {
          PType<?> ptype = nodePath.tail().getPType();
          node = DoNode.createOutputNode(target.toString(), target.getConverter(ptype), ptype);
          outputHandler.configureNode(node, target);
        }
        outputNodes.add(walkPath(nodePath.descendingIterator(), node));
      }
      allTargets.add(target);
    }

    Set<DoNode> mapSideNodes = Sets.newHashSet();
    if (mapSideNodePaths != null) {
      for (Target target : mapSideNodePaths.keySet()) {
        DoNode node = null;
        for (NodePath nodePath : mapSideNodePaths.get(target)) {
          if (node == null) {
            PType<?> ptype = nodePath.tail().getPType();
            node = DoNode.createOutputNode(target.toString(), target.getConverter(ptype), ptype);
            outputHandler.configureNode(node, target);
          }
          mapSideNodes.add(walkPath(nodePath.descendingIterator(), node));
        }
        allTargets.add(target);
      }
    }
    
    job.setMapperClass(CrunchMapper.class);
    List<DoNode> inputNodes;
    DoNode reduceNode = null;
    if (group != null) {
      job.setReducerClass(CrunchReducer.class);
      List<DoNode> reduceNodes = Lists.newArrayList(outputNodes);
      serialize(reduceNodes, conf, workingPath, NodeContext.REDUCE);
      reduceNode = reduceNodes.get(0);

      if (combineFnTable != null) {
        job.setCombinerClass(CrunchCombiner.class);
        DoNode combinerInputNode = group.createDoNode();
        DoNode combineNode = combineFnTable.createCombineNode();
        combineNode.addChild(group.getGroupingNode());
        combinerInputNode.addChild(combineNode);
        serialize(ImmutableList.of(combinerInputNode), conf, workingPath, NodeContext.COMBINE);
      }

      group.configureShuffle(job);

      DoNode mapOutputNode = group.getGroupingNode();
      Set<DoNode> mapNodes = Sets.newHashSet(mapSideNodes);
      for (NodePath nodePath : mapNodePaths) {
        // Advance these one step, since we've already configured
        // the grouping node, and the BaseGroupedTable is the tail
        // of the NodePath.
        Iterator<PCollectionImpl<?>> iter = nodePath.descendingIterator();
        iter.next();
        mapNodes.add(walkPath(iter, mapOutputNode));
      }
      inputNodes = Lists.newArrayList(mapNodes);
    } else { // No grouping
      job.setNumReduceTasks(0);
      inputNodes = Lists.newArrayList(outputNodes);
    }
    job.setOutputFormatClass(CrunchOutputFormat.class);
    serialize(inputNodes, conf, workingPath, NodeContext.MAP);

    if (inputNodes.size() == 1) {
      DoNode inputNode = inputNodes.get(0);
      inputNode.getSource().configureSource(job, -1);
    } else {
      for (int i = 0; i < inputNodes.size(); i++) {
        inputNodes.get(i).getSource().configureSource(job, i);
      }
      job.setInputFormatClass(CrunchInputFormat.class);
    }
    JobNameBuilder jobNameBuilder = createJobNameBuilder(conf, pipeline.getName(), inputNodes, reduceNode, numOfJobs);

    return new CrunchControlledJob(
        jobID,
        job,
        jobNameBuilder,
        allTargets,
        new CrunchJobHooks.PrepareHook(job),
        new CrunchJobHooks.CompletionHook(job, outputPath, outputHandler.getMultiPaths(), group == null));
  }

  private void serialize(List<DoNode> nodes, Configuration conf, Path workingPath, NodeContext context)
      throws IOException {
    List<RTNode> rtNodes = Lists.newArrayList();
    for (DoNode node : nodes) {
      rtNodes.add(node.toRTNode(true, conf, context));
    }
    Path path = new Path(workingPath, context.toString());
    DistCache.write(conf, path, rtNodes);
  }

  private JobNameBuilder createJobNameBuilder(
      Configuration conf,
      String pipelineName,
      List<DoNode> mapNodes,
      DoNode reduceNode,
      int numOfJobs) {
    JobNameBuilder builder = new JobNameBuilder(conf, pipelineName, jobID, numOfJobs);
    builder.visit(mapNodes);
    if (reduceNode != null) {
      builder.visit(reduceNode);
    }
    return builder;
  }

  private DoNode walkPath(Iterator<PCollectionImpl<?>> iter, DoNode working) {
    while (iter.hasNext()) {
      PCollectionImpl<?> collect = iter.next();
      if (combineFnTable != null && !(collect instanceof PGroupedTableImpl)) {
        combineFnTable = null;
      } else if (collect instanceof DoTable && ((DoTable<?, ?>) collect).hasCombineFn()) {
        combineFnTable = (DoTable<?, ?>) collect;
      }
      if (!nodes.containsKey(collect)) {
        nodes.put(collect, ((MRCollection) collect).createDoNode());
      }
      DoNode parent = nodes.get(collect);
      parent.addChild(working);
      working = parent;
    }
    return working;
  }
}
