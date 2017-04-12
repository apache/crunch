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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.crunch.Pair;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;

/**
 * Writes <a href="http://www.graphviz.org">Graphviz</a> dot files to illustrate
 * the topology of Crunch pipelines.
 */
public class DotfileWriter {

  // Maximum length that a node name may have in the produced dot file
  static final int MAX_NODE_NAME_LENGTH = 300;

  /** The types of tasks within a MapReduce job. */
  enum MRTaskType { MAP, REDUCE }

  private Set<JobPrototype> jobPrototypes = Sets.newHashSet();
  private HashMultimap<Pair<JobPrototype, MRTaskType>, String> jobNodeDeclarations = HashMultimap.create();
  private Set<String> globalNodeDeclarations = Sets.newHashSet();
  private Set<String> nodePathChains = Sets.newHashSet();

  /**
   * Format the declaration of a node based on a PCollection.
   *
   * @param pcollectionImpl PCollection for which a node will be declared
   * @param jobPrototype The job containing the PCollection
   * @return The node declaration
   */
  String formatPCollectionNodeDeclaration(PCollectionImpl<?> pcollectionImpl, JobPrototype jobPrototype) {
    String shape = "box";
    if (pcollectionImpl instanceof InputCollection) {
      shape = "folder";
    }

    String size = "";
    try {
      DecimalFormatSymbols formatSymbols = new DecimalFormatSymbols(Locale.ENGLISH);
      DecimalFormat formatter = new DecimalFormat("#,###.##", formatSymbols);
      size = " " + formatter.format(pcollectionImpl.getSize()/1024.0/1024.0) + " Mb";
    } catch (Exception e) {
      // Just skip those that don't have a size
    }

    if (pcollectionImpl instanceof PGroupedTableImpl) {
      int numReduceTasks = ((PGroupedTableImpl) pcollectionImpl).getNumReduceTasks();
      if (numReduceTasks > 0) {
        PGroupedTableImpl pGroupedTable = (PGroupedTableImpl) pcollectionImpl;
        String setByUser = pGroupedTable.isNumReduceTasksSetByUser() ? "Manual" : "Automatic";
        size += " (" + pGroupedTable.getNumReduceTasks() + " " + setByUser + " reducers)";
      }
    }

    return String.format("%s [label=\"%s%s\" shape=%s];",
        formatPCollection(pcollectionImpl, jobPrototype),
        limitNodeNameLength(pcollectionImpl.getName()),
        size,
        shape);
  }

  /**
   * Format a Target as a node declaration.
   *
   * @param target A Target used within a MapReduce pipeline
   * @return The global node declaration for the Target
   */
  String formatTargetNodeDeclaration(Target target) {
    String nodeName = limitNodeNameLength(target.toString());
    return String.format("\"%s\" [label=\"%s\" shape=folder];", nodeName, nodeName);
  }

  /**
   * Format a PCollectionImpl into a format to be used for dot files.
   *
   * @param pcollectionImpl The PCollectionImpl to be formatted
   * @param jobPrototype The job containing the PCollection
   * @return The dot file formatted representation of the PCollectionImpl
   */
  String formatPCollection(PCollectionImpl<?> pcollectionImpl, JobPrototype jobPrototype) {
    if (pcollectionImpl instanceof InputCollection) {
      InputCollection<?> inputCollection = (InputCollection<?>) pcollectionImpl;
      return String.format("\"%s\"", limitNodeNameLength(inputCollection.getSource().toString()));
    }
    return String.format("\"%s\"",
        limitNodeNameLength(
            String.format("%s@%d@%d", pcollectionImpl.getName(), pcollectionImpl.hashCode(), jobPrototype.hashCode())));
  }

  /**
   * Format a collection of node strings into dot file syntax.
   *
   * @param nodeCollection Collection of chained node strings
   * @return The dot-formatted chain of nodes
   */
  String formatNodeCollection(List<String> nodeCollection) {
    return formatNodeCollection(nodeCollection, ImmutableMap.<String, String>of());
  }

  /**
   * Limit a node name length down to {@link #MAX_NODE_NAME_LENGTH}, to ensure valid (and readable) dot files. If the
   * name is already less than or equal to the maximum length, it will be returned untouched.
   *
   * @param nodeName node name to be limited in length
   * @return the abbreviated node name if it was longer than the given maximum allowable length
   */
  static String limitNodeNameLength(String nodeName) {
    if (nodeName.length() <= MAX_NODE_NAME_LENGTH) {
      return nodeName;
    }
    String hashString = Integer.toString(nodeName.hashCode());
    return String.format("%s@%s",
        StringUtils.abbreviate(nodeName, MAX_NODE_NAME_LENGTH - (hashString.length() + 1)), hashString);
  }

  /**
   * Format a collection of node strings into dot file syntax.
   *
   * @param nodeCollection Collection of chained node strings
   * @param edgeAttributes map of attribute names and values to be applied to the edge
   * @return The dot-formatted chain of nodes
   */
  String formatNodeCollection(List<String> nodeCollection, Map<String,String> edgeAttributes) {
    String edgeAttributeString = "";
    if (!edgeAttributes.isEmpty()) {
      edgeAttributeString = String.format(" [%s]",
        Joiner.on(' ').withKeyValueSeparator("=").join(edgeAttributes));
    }
    return String.format("%s%s;", Joiner.on(" -> ").join(nodeCollection), edgeAttributeString);
  }

  /**
   * Format a NodePath in dot file syntax.
   *
   * @param nodePath The node path to be formatted
   * @param jobPrototype The job containing the NodePath
   * @return The dot file representation of the node path
   */
  List<String> formatNodePath(NodePath nodePath, JobPrototype jobPrototype) {
    List<String> formattedNodePaths = Lists.newArrayList();

    List<PCollectionImpl<?>> pcollections = ImmutableList.copyOf(nodePath);
    for (int collectionIndex = 1; collectionIndex < pcollections.size(); collectionIndex++){
      String fromNode = formatPCollection(pcollections.get(collectionIndex - 1), jobPrototype);
      String toNode = formatPCollection(pcollections.get(collectionIndex), jobPrototype);
      formattedNodePaths.add(formatNodeCollection(ImmutableList.of(fromNode, toNode)));
    }

    // Add SourceTarget dependencies, if any
    for (PCollectionImpl<?> pcollection : pcollections) {
      Set<SourceTarget<?>> targetDeps = pcollection.getParallelDoOptions().getSourceTargets();
      if (!targetDeps.isEmpty()) {
        String toNode = formatPCollection(pcollection, jobPrototype);
        for(Target target : targetDeps) {
          globalNodeDeclarations.add(formatTargetNodeDeclaration(target));
          String fromNode = String.format("\"%s\"", limitNodeNameLength(target.toString()));
          formattedNodePaths.add(
            formatNodeCollection(
              ImmutableList.of(fromNode, toNode),
              ImmutableMap.of("style", "dashed")));
        }
      }
    }
    return formattedNodePaths;
  }

  /**
   * Add a NodePath to be formatted as a list of node declarations within a
   * single job.
   *
   * @param jobPrototype The job containing the node path
   * @param nodePath The node path to be formatted
   */
  void addNodePathDeclarations(JobPrototype jobPrototype, NodePath nodePath) {
    boolean groupingEncountered = false;
    for (PCollectionImpl<?> pcollectionImpl : nodePath) {
      if (pcollectionImpl instanceof InputCollection) {
        globalNodeDeclarations.add(formatPCollectionNodeDeclaration(pcollectionImpl, jobPrototype));
      } else {
        if (!groupingEncountered) {
          groupingEncountered = (pcollectionImpl instanceof PGroupedTableImpl);
        }

        MRTaskType taskType = groupingEncountered ? MRTaskType.REDUCE : MRTaskType.MAP;
        jobNodeDeclarations.put(Pair.of(jobPrototype, taskType),
            formatPCollectionNodeDeclaration(pcollectionImpl, jobPrototype));
      }
    }
  }

  /**
   * Add the chaining of a NodePath to the graph.
   *
   * @param nodePath The path to be formatted as a node chain in the dot file
   * @param jobPrototype The job containing the NodePath
   */
  void addNodePathChain(NodePath nodePath, JobPrototype jobPrototype) {
    for (String nodePathChain : formatNodePath(nodePath, jobPrototype)){
      this.nodePathChains.add(nodePathChain);
    }
  }

  /**
   * Get the graph attributes for a task-specific subgraph.
   *
   * @param taskType The type of task in the subgraph
   * @return Graph attributes
   */
  String getTaskGraphAttributes(MRTaskType taskType) {
    if (taskType == MRTaskType.MAP) {
      return "label = Map; color = blue;";
    } else {
      return "label = Reduce; color = red;";
    }
  }

  private void processNodePaths(JobPrototype jobPrototype, HashMultimap<Target, NodePath> nodePaths) {
    if (nodePaths != null) {
      for (Target target : nodePaths.keySet()) {
        globalNodeDeclarations.add(formatTargetNodeDeclaration(target));
        for (NodePath nodePath : nodePaths.get(target)) {
          addNodePathDeclarations(jobPrototype, nodePath);
          addNodePathChain(nodePath, jobPrototype);
          nodePathChains.add(formatNodeCollection(
              Lists.newArrayList(formatPCollection(nodePath.descendingIterator().next(), jobPrototype),
                  String.format("\"%s\"", limitNodeNameLength(target.toString())))));
        }
      }
    }
  }

  /**
   * Add the contents of a {@link JobPrototype} to the graph describing a
   * pipeline.
   *
   * @param jobPrototype A JobPrototype representing a portion of a MapReduce
   *          pipeline
   */
  public void addJobPrototype(JobPrototype jobPrototype) {
    jobPrototypes.add(jobPrototype);
    if (!jobPrototype.isMapOnly()) {
      for (NodePath nodePath : jobPrototype.getMapNodePaths()) {
        addNodePathDeclarations(jobPrototype, nodePath);
        addNodePathChain(nodePath, jobPrototype);
      }
      processNodePaths(jobPrototype, jobPrototype.getMapSideNodePaths());
    }
    processNodePaths(jobPrototype, jobPrototype.getTargetsToNodePaths());
  }

  /**
   * Build up the full dot file containing the description of a MapReduce
   * pipeline.
   *
   * @return Graphviz dot file contents
   */
  public String buildDotfile() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("digraph G {\n");

    for (String globalDeclaration : globalNodeDeclarations) {
      stringBuilder.append(String.format("  %s\n", globalDeclaration));
    }

    for (JobPrototype jobPrototype : jobPrototypes){
      // Must prefix subgraph name with "cluster", otherwise its border won't render. I don't know why.
      StringBuilder jobProtoStringBuilder = new StringBuilder();
      jobProtoStringBuilder.append(String.format("  subgraph \"cluster-job%d\" {\n", jobPrototype.getJobID()));
      jobProtoStringBuilder.append(String.format("    label=\"Crunch Job %d\";\n", jobPrototype.getJobID()));
      for (MRTaskType taskType : MRTaskType.values()){
        Pair<JobPrototype,MRTaskType> jobTaskKey = Pair.of(jobPrototype, taskType);
        if (jobNodeDeclarations.containsKey(jobTaskKey)){
          jobProtoStringBuilder.append(String.format(
              "    subgraph \"cluster-job%d-%s\" {\n", jobPrototype.getJobID(), taskType.name().toLowerCase()));
          jobProtoStringBuilder.append(String.format("      %s\n", getTaskGraphAttributes(taskType)));
          for (String declarationEntry : jobNodeDeclarations.get(jobTaskKey)){
            jobProtoStringBuilder.append(String.format("      %s\n", declarationEntry));
          }
          jobProtoStringBuilder.append("    }\n");
        }
      }
      jobProtoStringBuilder.append("  }\n");
      stringBuilder.append(jobProtoStringBuilder.toString());
    }

    for (String nodePathChain : nodePathChains) {
      stringBuilder.append(String.format("  %s\n", nodePathChain));
    }

    stringBuilder.append("}\n");
    return stringBuilder.toString();
  }



}
