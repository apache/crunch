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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.Source;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;
import org.apache.crunch.impl.mr.plan.DotfileWriter.MRTaskType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DotfileWriterTest {

  private DotfileWriter dotfileWriter;

  @Before
  public void setUp() {
    dotfileWriter = new DotfileWriter();
  }

  @Test
  public void testFormatPCollectionNodeDeclaration() {
    PCollectionImpl<?> pcollectionImpl = mock(PCollectionImpl.class);
    JobPrototype jobPrototype = mock(JobPrototype.class);
    when(pcollectionImpl.getName()).thenReturn("collection");
    when(pcollectionImpl.getSize()).thenReturn(1024L * 500L);

    assertEquals("\"collection@" + pcollectionImpl.hashCode() + "@" + jobPrototype.hashCode()
        + "\" [label=\"collection 0.49 Mb\" shape=box];",
        dotfileWriter.formatPCollectionNodeDeclaration(pcollectionImpl, jobPrototype));
  }

  @Test
  public void testFormatPCollectionNodeDeclaration_InputPCollection() {
    InputCollection<?> inputCollection = mock(InputCollection.class, Mockito.RETURNS_DEEP_STUBS);
    JobPrototype jobPrototype = mock(JobPrototype.class);
    when(inputCollection.getName()).thenReturn("input");
    when(inputCollection.getSource().toString()).thenReturn("source");
    when(inputCollection.getSize()).thenReturn(1024L * 1024L * 1729L);

    assertEquals("\"source\" [label=\"input 1,729 Mb\" shape=folder];",
        dotfileWriter.formatPCollectionNodeDeclaration(inputCollection, jobPrototype));
  }

  @Test
  public void testFormatPGroupedTableImplDeclarationAutomatic() {
    PGroupedTableImpl<?,?> inputCollection = mock(PGroupedTableImpl.class, Mockito.RETURNS_DEEP_STUBS);
    JobPrototype jobPrototype = mock(JobPrototype.class);
    when(inputCollection.getName()).thenReturn("GBK");
    when(inputCollection.getSize()).thenReturn(1024L * 1024L * 1729L);
    when(inputCollection.getNumReduceTasks()).thenReturn(10);

    String expected = "\"GBK@" + inputCollection.hashCode() + "@" + jobPrototype.hashCode() + "\" [label=\"GBK " +
      "1,729 Mb (10 Automatic reducers)\" shape=box];";

    assertEquals(expected, dotfileWriter.formatPCollectionNodeDeclaration(inputCollection, jobPrototype));
  }

  @Test
  public void testFormatPGroupedTableImplDeclarationManual() {
    PGroupedTableImpl<?,?> inputCollection = mock(PGroupedTableImpl.class, Mockito.RETURNS_DEEP_STUBS);
    JobPrototype jobPrototype = mock(JobPrototype.class);
    when(inputCollection.getName()).thenReturn("collection");
    when(inputCollection.getSize()).thenReturn(1024L * 1024L * 1729L);
    when(inputCollection.getNumReduceTasks()).thenReturn(50);
    when(inputCollection.isNumReduceTasksSetByUser()).thenReturn(true);

    String expected = "\"collection@" + inputCollection.hashCode() + "@" + jobPrototype.hashCode() + "\" [label=\"collection " +
      "1,729 Mb (50 Manual reducers)\" shape=box];";

    assertEquals(expected, dotfileWriter.formatPCollectionNodeDeclaration(inputCollection, jobPrototype));
  }

  @Test
  public void testFormatTargetNodeDeclaration() {
    Target target = mock(Target.class);
    when(target.toString()).thenReturn("target/path");

    assertEquals("\"target/path\" [label=\"target/path\" shape=folder];",
        dotfileWriter.formatTargetNodeDeclaration(target));
  }

  @Test
  public void testFormatPCollection() {
    PCollectionImpl<?> pcollectionImpl = mock(PCollectionImpl.class);
    JobPrototype jobPrototype = mock(JobPrototype.class);
    when(pcollectionImpl.getName()).thenReturn("collection");

    assertEquals("\"collection@" + pcollectionImpl.hashCode() + "@" + jobPrototype.hashCode() + "\"",
        dotfileWriter.formatPCollection(pcollectionImpl, jobPrototype));
  }

  @Test
  public void testFormatPCollection_InputCollection() {
    InputCollection<Object> inputCollection = mock(InputCollection.class);
    Source<Object> source = mock(Source.class);
    JobPrototype jobPrototype = mock(JobPrototype.class);
    when(source.toString()).thenReturn("mocksource");
    when(inputCollection.getSource()).thenReturn(source);

    assertEquals("\"mocksource\"", dotfileWriter.formatPCollection(inputCollection, jobPrototype));
  }

  @Test
  public void testFormatNodeCollection() {
    List<String> nodeCollection = Lists.newArrayList("one", "two", "three");
    assertEquals("one -> two -> three;", dotfileWriter.formatNodeCollection(nodeCollection));
  }

  @Test
  public void testFormatNodeCollection_WithStyles() {
    List<String> nodeCollection = Lists.newArrayList("one", "two");
    assertEquals(
      "one -> two [style=dotted];",
      dotfileWriter.formatNodeCollection(nodeCollection, ImmutableMap.of("style", "dotted")));
  }

  @Test
  public void testFormatNodePath() {
    PCollectionImpl<?> tail = mock(PCollectionImpl.class);
    PCollectionImpl<?> head = mock(PCollectionImpl.class);
    JobPrototype jobPrototype = mock(JobPrototype.class);
    ParallelDoOptions doOptions = ParallelDoOptions.builder().build();

    when(tail.getName()).thenReturn("tail");
    when(head.getName()).thenReturn("head");
    when(tail.getParallelDoOptions()).thenReturn(doOptions);
    when(head.getParallelDoOptions()).thenReturn(doOptions);

    NodePath nodePath = new NodePath(tail);
    nodePath.close(head);

    assertEquals(
        Lists.newArrayList("\"head@" + head.hashCode() + "@" + jobPrototype.hashCode() + "\" -> \"tail@"
            + tail.hashCode() + "@" + jobPrototype.hashCode() + "\";"),
        dotfileWriter.formatNodePath(nodePath, jobPrototype));
  }

  @Test
  public void testFormatNodePathWithTargetDependencies() {
    PCollectionImpl<?> tail = mock(PCollectionImpl.class);
    PCollectionImpl<?> head = mock(PCollectionImpl.class);
    SourceTarget<?> srcTarget = mock(SourceTarget.class);
    JobPrototype jobPrototype = mock(JobPrototype.class);

    ParallelDoOptions tailOptions = ParallelDoOptions.builder().sourceTargets(srcTarget).build();
    ParallelDoOptions headOptions = ParallelDoOptions.builder().build();
    when(srcTarget.toString()).thenReturn("target");
    when(tail.getName()).thenReturn("tail");
    when(head.getName()).thenReturn("head");
    when(tail.getParallelDoOptions()).thenReturn(tailOptions);
    when(head.getParallelDoOptions()).thenReturn(headOptions);

    NodePath nodePath = new NodePath(tail);
    nodePath.close(head);

    assertEquals(
        ImmutableList.of("\"head@" + head.hashCode() + "@" + jobPrototype.hashCode() + "\" -> \"tail@"
            + tail.hashCode() + "@" + jobPrototype.hashCode() + "\";",
            "\"target\" -> \"tail@" + tail.hashCode() + "@" + jobPrototype.hashCode() + "\" [style=dashed];"),
        dotfileWriter.formatNodePath(nodePath, jobPrototype));
  }


  @Test
  public void testGetTaskGraphAttributes_Map() {
    assertEquals("label = Map; color = blue;", dotfileWriter.getTaskGraphAttributes(MRTaskType.MAP));
  }

  @Test
  public void testGetTaskGraphAttributes_Reduce() {
    assertEquals("label = Reduce; color = red;", dotfileWriter.getTaskGraphAttributes(MRTaskType.REDUCE));
  }

  @Test
  public void testLimitNodeNameLength_AlreadyWithinLimit() {
    String nodeName = "within_limit";
    assertEquals(nodeName, DotfileWriter.limitNodeNameLength(nodeName));
  }

  @Test
  public void testLimitNodeNameLength_OverLimit() {
    String nodeName = Strings.repeat("x", DotfileWriter.MAX_NODE_NAME_LENGTH + 1);
    String abbreviated = DotfileWriter.limitNodeNameLength(nodeName);
    assertEquals(DotfileWriter.MAX_NODE_NAME_LENGTH, abbreviated.length());
    assertTrue(abbreviated.startsWith("xxxxx"));
  }
}
