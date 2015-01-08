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
import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.impl.mr.MRJob;
import org.apache.crunch.impl.mr.run.NodeContext;
import org.apache.crunch.impl.mr.run.RTNode;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.CrunchOutputs.OutputConfig;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.types.Converter;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Writes <a href="http://www.graphviz.org">Graphviz</a> dot files to illustrate the topology of Crunch pipelines.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DotfileWriterRTNodes extends CommonDotfileWriter {

  private static final String GREEN = "green";
  private static final String RED = "red";
  private static final String CYAN = "cyan";
  private static final String BLUE = "blue";
  private static final String BLACK = "black";

  private List<MRJob> mrJobs;

  public DotfileWriterRTNodes(List<MRJob> mrJobs) {
    super();
    this.mrJobs = mrJobs;
  }

  private String getId(RTNode rtNode) {
    return format("\"%s@%d\"", rtNode.getNodeName(), rtNode.hashCode());
  }

  private String getOutputNameId(String outputName, MRJob mrJob) {
    return format("\"%s@%s\"", outputName, mrJob.getJobID());
  }

  private String getId(FormatBundle bundle, MRJob mrJob) {
    String name = (bundle == null) ? "-" : bundle.getName();
    return format("\"%s@%s\"", name, mrJob.getJobID());
  }

  private String formatConvertor(Converter converter) {
    StringBuffer sb = new StringBuffer();
    sb.append(className(converter));
    if (converter != null) {
      if (!converter.applyPTypeTransforms()) {
        sb.append(" (applyPTypeTransforms = ").append(converter.applyPTypeTransforms()).append(")");
      }
      sb.append("[").append(converter.getKeyClass().getSimpleName()).append(", ")
          .append(converter.getValueClass().getSimpleName()).append("]");
    }
    return sb.toString();
  }

  private String formatRTNode(RTNode rtNode) {
    return format("%s [label=\"{{%s | %s} | %s | %s | { %s | %s } }\" shape=record; color = black;];\n", getId(rtNode),
        label(rtNode.getNodeName()), label(rtNode.getOutputName()), className(rtNode.getDoFn()),
        formatPType(rtNode.getPType()), formatConvertor(rtNode.getInputConverter()),
        formatConvertor(rtNode.getOutputConverter()));
  }

  private void formatRTNodeTree(RTNode parentRTNode) {

    contentBuilder.append(formatRTNode(parentRTNode));

    if (!isEmpty(parentRTNode.getChildren())) {
      for (RTNode child : parentRTNode.getChildren()) {
        // process child nodes
        formatRTNodeTree(child);
        // link parent to child node
        link(getId(parentRTNode), getId(child), BLACK);
      }
    }
  }

  private List<RTNode> formatMRJobTask(Configuration configuration, int jobId, NodeContext nodeContext, String color) {

    List<RTNode> rtNodes = getRTNodes(configuration, nodeContext);
    if (rtNodes == null)
      return null;

    contentBuilder.append("subgraph \"cluster-job" + jobId + "_" + nodeContext + "\" {\n");
    contentBuilder.append(" label=\"" + nodeContext + "\"; color=" + color + "; fontsize=14;\n");

    for (RTNode rtn : rtNodes) {
      formatRTNodeTree(rtn);
    }
    contentBuilder.append("}\n");

    return rtNodes;
  }

  private void formatJobOutputs(Map<String, OutputConfig> namedOutputs, MRJob mrJob) {

    contentBuilder.append("subgraph \"cluster-output_" + mrJob.getJobID() + "\" {\n");
    contentBuilder.append(" label=\"OUTPUTS\"; fontsize=14; color= magenta;\n");

    for (Entry<String, OutputConfig> entry : namedOutputs.entrySet()) {
      String output = format("%s [label=\"{%s | %s | { %s | %s } }\" shape=record; color = %s];\n",
          getOutputNameId(entry.getKey(), mrJob), entry.getKey(), entry.getValue().bundle.getName(),
          entry.getValue().keyClass.getSimpleName(), entry.getValue().valueClass.getSimpleName(), BLACK);

      contentBuilder.append(output);
    }

    contentBuilder.append("}\n");
  }

  private void formatJobInputs(Map<FormatBundle, Map<Integer, List<Path>>> inputFormatNodeMap, MRJob mrJob, String color) {

    contentBuilder.append("subgraph \"cluster-inputs_" + mrJob.getJobID() + "\" {\n");
    contentBuilder.append(" label=\"INPUTS\"; fontsize=14; color= " + color + ";\n");

    for (Entry<FormatBundle, Map<Integer, List<Path>>> entry : inputFormatNodeMap.entrySet()) {

      FormatBundle bundle = entry.getKey();

      ArrayList<String> inList = new ArrayList<String>();
      for (Entry<Integer, List<Path>> value : entry.getValue().entrySet()) {
        inList.add(format("{ %s | %s}", value.getKey(), value.getValue()));
      }

      contentBuilder.append(format("%s [label=\"{ %s | %s}\" shape=record; color = %s];\n", getId(bundle, mrJob),
          bundle.getName(), Joiner.on("|").join(inList), BLACK));
    }

    contentBuilder.append("}\n");
  }

  private FormatBundle findFormatBundleByNodeIndex(Map<FormatBundle, Map<Integer, List<Path>>> inputFormatNodeMap,
      int nodeIndex) {
    for (Entry<FormatBundle, Map<Integer, List<Path>>> entry : inputFormatNodeMap.entrySet()) {
      if (entry.getValue().containsKey(nodeIndex)) {
        return entry.getKey();
      }
      if (nodeIndex == 0 && entry.getValue().containsKey(-1)) {
        return entry.getKey();
      }
    }
    return null;
  }

  private List<RTNode> leafs(List<RTNode> rtNodes) {

    ArrayList<RTNode> tails = Lists.newArrayListWithExpectedSize(rtNodes.size());

    for (RTNode node : rtNodes) {
      tails.addAll(leafs(node));
    }
    return tails;
  }

  private List<RTNode> leafs(RTNode rtNode) {

    List<RTNode> leafs = Lists.newArrayList();

    if (rtNode.isLeafNode()) {
      leafs.add(rtNode);
    } else {
      for (RTNode child : rtNode.getChildren()) {
        leafs.addAll(leafs(child));
      }
    }

    return leafs;
  }

  private static List<RTNode> getRTNodes(Configuration conf, NodeContext nodeContext) {
    Path path = new Path(new Path(conf.get(PlanningParameters.CRUNCH_WORKING_DIRECTORY)), nodeContext.toString());
    try {
      return (List<RTNode>) DistCache.read(conf, path);
    } catch (IOException e) {
      throw new CrunchRuntimeException("Could not read runtime node information", e);
    }
  }

  @Override
  protected void doBuildDiagram() {

    for (MRJob mrJob : mrJobs) {

      // TODO to find a way to handle job dependencies e.g mrJob.getDependentJobs()

      Configuration configuration = mrJob.getJob().getConfiguration();

      contentBuilder.append("subgraph \"cluster-job" + mrJob.getJobID() + "\" {\n");
      contentBuilder.append("    label=\"Crunch Job " + mrJob.getJobID() + "\" ;\n");

      List<RTNode> mapRTNodes = formatMRJobTask(configuration, mrJob.getJobID(), NodeContext.MAP, BLUE);
      List<RTNode> combineRTNodes = formatMRJobTask(configuration, mrJob.getJobID(), NodeContext.COMBINE, CYAN);
      List<RTNode> reduceRTNodes = formatMRJobTask(configuration, mrJob.getJobID(), NodeContext.REDUCE, RED);

      // Deserialize Job's inputs from the CRUNCH_INPUTS Configuration property.
      Map<FormatBundle, Map<Integer, List<Path>>> inputFormatNodeMap = CrunchInputs.getFormatNodeMap(mrJob.getJob());

      formatJobInputs(inputFormatNodeMap, mrJob, GREEN);

      // Link inputs to map RTNode tasks
      for (int mapNodeIndex = 0; mapNodeIndex < mapRTNodes.size(); mapNodeIndex++) {
        FormatBundle formatBundle = findFormatBundleByNodeIndex(inputFormatNodeMap, mapNodeIndex);
        RTNode rtNode = mapRTNodes.get(mapNodeIndex);
        link(getId(formatBundle, mrJob), getId(rtNode), BLACK);
      }

      // Deserialize Job's Outputs from the CRUNCH_OUTPUTS Configuration property.
      Map<String, OutputConfig> namedOutputs = CrunchOutputs.getNamedOutputs(configuration);

      formatJobOutputs(namedOutputs, mrJob);

      List<RTNode> mapLeafs = leafs(mapRTNodes);

      for (RTNode leafNode : mapLeafs) {
        String outputName = leafNode.getOutputName();
        if (StringUtils.isEmpty(outputName)) {
          if (!isEmpty(combineRTNodes)) {
            // If there is a combiner connect the map to the combiner and then the combiner to the reducer
            link(getId(leafNode), getId(combineRTNodes.get(0)), BLACK);
            link(getId(leafs(combineRTNodes).get(0)), getId(reduceRTNodes.get(0)), BLACK);
          } else {
            // connect
            link(getId(leafNode), getId(reduceRTNodes.get(0)), BLACK);
          }
        } else {
          link(getId(leafNode), getOutputNameId(outputName, mrJob), BLACK);
        }
      }

      if (!isEmpty(reduceRTNodes)) {
        List<RTNode> reduceTails = leafs(reduceRTNodes);
        for (RTNode tailNode : reduceTails) {
          String outputName = tailNode.getOutputName();
          if (StringUtils.isEmpty(outputName)) {
            throw new RuntimeException("Recue output RTNode with no named output! :" + tailNode);
          } else {
            link(getId(tailNode), getOutputNameId(outputName, mrJob), BLACK);
          }
        }
      }

      contentBuilder.append("}\n");

    }
  }

  @Override
  protected void doGetLegend(StringBuilder lsb) {
    lsb.append(
        "\"RTNodes\"  [label=\"{{RTNode Name | Output Name } | DoFn | PType | { Input Converter | Output Converter}}\"; shape=record;]\n")
        .append("\"Inputs\"  [label=\"{InputFormat Name | {Node Index | Path List}}\"; shape=record; color = green]\n")
        .append(
            "\"Outputs\"  [label=\"{Output Name | OutputFormat Name |{Key Class | Value Class}}\"; shape=record; color = magenta]\n")
        .append("\"Inputs\" -> \"RTNodes\" [style=invis];\n").append("\"RTNodes\" -> \"Outputs\" [style=invis];\n");

  }
}
