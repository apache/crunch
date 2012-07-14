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

import static org.junit.Assert.assertEquals;

import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

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
