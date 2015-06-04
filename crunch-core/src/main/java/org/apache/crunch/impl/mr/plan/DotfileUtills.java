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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.exec.MRExecutor;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Helper class that manages the dotfile generation lifecycle and configuring the dotfile debug context.
 */
public class DotfileUtills {

  private static final Logger LOG = LoggerFactory.getLogger(DotfileUtills.class);

  private final Class<?> jarClass;
  private final Configuration conf;

  private String rtNodesDotfile = "";
  private String basePlanGraphDotfile = "";
  private String splitGraphPlanDotfile = "";
  private String pcollectionLineageDotfile = "";
  private String planDotFile = "";

  DotfileUtills(Class<?> jarClass, Configuration conf) {
    this.jarClass = jarClass;
    this.conf = conf;
  }

  /**
   * Builds a lineage dotfile only if the dotfile-debug mode is enabled.
   */
  void buildLineageDotfile(Map<PCollectionImpl<?>, Set<Target>> outputs) {
    if (isDebugDotfilesEnabled(conf)) {
      try {
        pcollectionLineageDotfile = new DotfileWriterPCollectionLineage(outputs)
            .buildDiagram("PCollection Lineage Plan (" + jarClass.getSimpleName() + ")");
      } catch (Exception ex) {
        LOG.error("Problem creating debug dotfile:", ex);
      }
    }
  }

  /**
   * Builds the base graph dotfile only if the dotfile-debug mode is enabled.
   */
  void buildBaseGraphDotfile(Map<PCollectionImpl<?>, Set<Target>> outputs, Graph graph) {
    if (isDebugDotfilesEnabled(conf)) {
      try {
        basePlanGraphDotfile = new DotfileWriterGraph(graph, outputs, null).buildDiagram("Base Graph ("
            + jarClass.getSimpleName() + ")");
      } catch (Exception ex) {
        LOG.error("Problem creating debug dotfile:", ex);
      }
    }
  }

  /**
   * Builds a split graph dotfile only if the dotfile-debug mode is enabled.
   */
  void buildSplitGraphDotfile(Map<PCollectionImpl<?>, Set<Target>> outputs, Graph graph, List<List<Vertex>> components) {
    if (isDebugDotfilesEnabled(conf)) {
      try {
        splitGraphPlanDotfile = new DotfileWriterGraph(graph, outputs, components)
            .buildDiagram("Graph With Components (" + jarClass.getSimpleName() + ")");
      } catch (Exception ex) {
        LOG.error("Problem creating debug dotfile:", ex);
      }
    }
  }

  /**
   * Builds a RT node dotfile only if the dotfile-debug mode is enabled.
   */
  void buildRTNodesDotfile(MRExecutor exec) {
    if (isDebugDotfilesEnabled(conf)) {
      try {
        rtNodesDotfile = new DotfileWriterRTNodes(exec.getJobs()).buildDiagram("Run Time Plan ("
            + jarClass.getSimpleName() + ")");
      } catch (Exception ex) {
        LOG.error("Problem creating debug dotfile:", ex);
      }
    }
  }

  /**
   * Build the plan dotfile despite of the the dotfile-debug mode.
   * 
   * @throws IOException
   */
  void buildPlanDotfile(MRExecutor exec, Multimap<Target, JobPrototype> assignments, MRPipeline pipeline, int lastJobID) {
    try {
      DotfileWriter dotfileWriter = new DotfileWriter();

      for (JobPrototype proto : Sets.newHashSet(assignments.values())) {
        dotfileWriter.addJobPrototype(proto);
      }

      planDotFile = dotfileWriter.buildDotfile();

    } catch (Exception ex) {
      LOG.error("Problem creating debug dotfile:", ex);
    }
  }

  /**
   * Attach the generated dotfiles to the {@link MRExecutor} context!. Note that the planDotFile is always added!
   */
  void addDotfilesToContext(MRExecutor exec) {
    try {
      // The job plan is always enabled and set in the Configuration;
      conf.set(PlanningParameters.PIPELINE_PLAN_DOTFILE, planDotFile);
      exec.addNamedDotFile("jobplan", planDotFile);

      // Debug dotfiles are only stored if the configuration is set to enabled
      if (isDebugDotfilesEnabled(conf)) {
        exec.addNamedDotFile("rt_plan", rtNodesDotfile);
        exec.addNamedDotFile("base_graph_plan", basePlanGraphDotfile);
        exec.addNamedDotFile("split_graph_plan", splitGraphPlanDotfile);
        exec.addNamedDotFile("lineage_plan", pcollectionLineageDotfile);
      }
    } catch (Exception ex) {
      LOG.error("Problem creating debug dotfile:", ex);
    }
  }

  public static boolean isDebugDotfilesEnabled(Configuration conf) {
    return conf.getBoolean(PlanningParameters.DEBUG_DOTFILES_ENABLED, false)
        && conf.get(PlanningParameters.PIPELINE_DOTFILE_OUTPUT_DIR) != null;
  }

  public static void enableDebugDotfiles(Configuration conf) {
    conf.setBoolean(PlanningParameters.DEBUG_DOTFILES_ENABLED, true);
  }

  public static void disableDebugDotfilesEnabled(Configuration conf) {
    conf.setBoolean(PlanningParameters.DEBUG_DOTFILES_ENABLED, false);
  }

  public static void setPipelineDotfileOutputDir(Configuration conf, String outputDir) {
    conf.set(PlanningParameters.PIPELINE_DOTFILE_OUTPUT_DIR, outputDir);
  }

  public static String getPipelineDotfileOutputDir(Configuration conf) {
    return conf.get(PlanningParameters.PIPELINE_DOTFILE_OUTPUT_DIR);
  }
}
