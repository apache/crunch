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

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Visitor that traverses the {@code DoNode} instances in a job and builds
 * a String that identifies the stages of the pipeline that belong to
 * this job.
 */
public class JobNameBuilder {
  
  private static final Joiner JOINER = Joiner.on("+");
  private static final Joiner CHILD_JOINER = Joiner.on("/");
  
  private String pipelineName;
  List<String> rootStack = Lists.newArrayList();
  
  public JobNameBuilder(final String pipelineName){
    this.pipelineName = pipelineName;
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
    return String.format("%s: %s", pipelineName, JOINER.join(rootStack));
  }
}
