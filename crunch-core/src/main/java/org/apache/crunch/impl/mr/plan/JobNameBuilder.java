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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

/**
 * Visitor that traverses the {@code DoNode} instances in a job and builds a
 * String that identifies the stages of the pipeline that belong to this job.
 */
public class JobNameBuilder {

  private static final Joiner JOINER = Joiner.on("+");
  private static final Joiner CHILD_JOINER = Joiner.on("/");
  private static final int DEFAULT_JOB_NAME_MAX_STACK_LENGTH = 60;

  private final String pipelineName;
  private final int jobID;
  private int jobSequence;
  private final int numOfJobs;
  List<String> rootStack = Lists.newArrayList();
  private final int maxStackNameLength;

  public JobNameBuilder(Configuration conf, String pipelineName, int jobID, int numOfJobs) {
    this.pipelineName = pipelineName;
    this.jobID = jobID;
    this.numOfJobs = numOfJobs;
    this.maxStackNameLength = conf.getInt(
        PlanningParameters.JOB_NAME_MAX_STACK_LENGTH, DEFAULT_JOB_NAME_MAX_STACK_LENGTH);
  }

  public JobNameBuilder jobSequence(int jobSequence) {
    this.jobSequence = jobSequence;
    return this;
  }

  public void visit(DoNode node) {
    visit(node, rootStack);
  }

  public void visit(List<DoNode> nodes) {
    visit(nodes, rootStack);
  }

  private void visit(List<DoNode> nodes, List<String> stack) {
    if (nodes.size() == 1) {
      visit(nodes.get(0), stack);
    } else {
      List<String> childStack = Lists.newArrayList();
      for (int i = 0; i < nodes.size(); i++) {
        DoNode node = nodes.get(i);
        List<String> subStack = Lists.newArrayList();
        visit(node, subStack);
        if (!subStack.isEmpty()) {
          childStack.add("[" + JOINER.join(subStack) + "]");
        }
      }
      if (!childStack.isEmpty()) {
        stack.add("[" + CHILD_JOINER.join(childStack) + "]");
      }
    }
  }

  private void visit(DoNode node, List<String> stack) {
    String name = node.getName();
    if (!name.isEmpty()) {
      stack.add(node.getName());
    }
    visit(node.getChildren(), stack);
  }

  public String build() {
    return String.format("%s: %s ID=%d (%d/%d)",
        pipelineName,
        shortenRootStackName(JOINER.join(rootStack), maxStackNameLength),
        jobID,
        jobSequence,
        numOfJobs);
  }

  private static String shortenRootStackName(String s, int len) {
    int n = s.length();
    if (len <= 3) {
      return "...";
    }
    if (n <= len) {
      return s;
    }
    return s.substring(0, len - 3) + "...";
  }
}
