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

/**
 * Collection of Configuration keys and various constants used when planning MapReduce jobs for a
 * pipeline.
 */
public class PlanningParameters {

  public static final String MULTI_OUTPUT_PREFIX = "out";

  public static final String CRUNCH_WORKING_DIRECTORY = "crunch.work.dir";

  /**
   * Configuration key under which a <a href="http://www.graphviz.org">DOT</a> file containing the
   * pipeline job graph is stored by the planner.
   */
  public static final String PIPELINE_PLAN_DOTFILE = "crunch.planner.dotfile";

  private PlanningParameters() {
  }
}
