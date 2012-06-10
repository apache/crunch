package com.cloudera.crunch.impl.mr.plan;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.junit.Test;

import com.cloudera.crunch.io.At;
import com.cloudera.crunch.types.writable.Writables;

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
