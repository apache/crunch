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

import java.util.List;
import java.util.Set;

import org.apache.crunch.Pair;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Writes <a href="http://www.graphviz.org">Graphviz</a> dot files to illustrate
 * the topology of Crunch pipelines.
 */
public class DotfileWriter {
  
  /** The types of tasks within a MapReduce job. */
  enum MRTaskType { MAP, REDUCE };

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
    return String.format("%s [label=\"%s\" shape=%s];", formatPCollection(pcollectionImpl, jobPrototype), pcollectionImpl.getName(),
        shape);
  }

  /**
   * Format a Target as a node declaration.
   * 
   * @param target A Target used within a MapReduce pipeline
   * @return The global node declaration for the Target
   */
  String formatTargetNodeDeclaration(Target target) {
    return String.format("\"%s\" [label=\"%s\" shape=folder];", target.toString(), target.toString());
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
      return String.format("\"%s\"", inputCollection.getSource());
    }
    return String.format("\"%s@%d@%d\"", pcollectionImpl.getName(), pcollectionImpl.hashCode(), jobPrototype.hashCode());
  }

  /**
   * Format a collection of node strings into dot file syntax.
   * 
   * @param nodeCollection Collection of chained node strings
   * @return The dot-formatted chain of nodes
   */
  String formatNodeCollection(List<String> nodeCollection) {
    return String.format("%s;", Joiner.on(" -> ").join(nodeCollection));
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
    
    List<PCollectionImpl<?>> pcollections = Lists.newArrayList(nodePath);
    for (int collectionIndex = 1; collectionIndex < pcollections.size(); collectionIndex++){
      String fromNode = formatPCollection(pcollections.get(collectionIndex - 1), jobPrototype);
      String toNode = formatPCollection(pcollections.get(collectionIndex), jobPrototype);
      formattedNodePaths.add(formatNodeCollection(Lists.newArrayList(fromNode, toNode)));
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
        if (!groupingEncountered){
          groupingEncountered = (pcollectionImpl instanceof PGroupedTableImpl);
        }

        MRTaskType taskType = groupingEncountered ? MRTaskType.REDUCE : MRTaskType.MAP;
        jobNodeDeclarations.put(Pair.of(jobPrototype, taskType), formatPCollectionNodeDeclaration(pcollectionImpl, jobPrototype));
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
    }

    HashMultimap<Target, NodePath> targetsToNodePaths = jobPrototype.getTargetsToNodePaths();
    for (Target target : targetsToNodePaths.keySet()) {
      globalNodeDeclarations.add(formatTargetNodeDeclaration(target));
      for (NodePath nodePath : targetsToNodePaths.get(target)) {
        addNodePathDeclarations(jobPrototype, nodePath);
        addNodePathChain(nodePath, jobPrototype);
        nodePathChains.add(formatNodeCollection(Lists.newArrayList(formatPCollection(nodePath.descendingIterator()
            .next(), jobPrototype), String.format("\"%s\"", target.toString()))));
      }
    }
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
