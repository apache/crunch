/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cloudera.crunch.types.writable.Writables;
import com.google.common.collect.Lists;

public class JobNameBuilderTest {

  @Test
  public void testBuild() {
    final String pipelineName = "PipelineName";
    final String nodeName = "outputNode";
    DoNode doNode = DoNode.createOutputNode(nodeName, Writables.strings());
    JobNameBuilder jobNameBuilder = new JobNameBuilder(pipelineName);
    jobNameBuilder.visit(Lists.newArrayList(doNode));
    String jobName = jobNameBuilder.build();
    
    assertEquals(String.format("%s: %s", pipelineName, nodeName), jobName);
  }

}
