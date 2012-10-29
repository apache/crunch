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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.plan.DotfileWriter.MRTaskType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

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

    assertEquals("\"collection@" + pcollectionImpl.hashCode() + "@" + jobPrototype.hashCode()
        + "\" [label=\"collection\" shape=box];",
        dotfileWriter.formatPCollectionNodeDeclaration(pcollectionImpl, jobPrototype));
  }

  @Test
  public void testFormatPCollectionNodeDeclaration_InputPCollection() {
    InputCollection<?> inputCollection = mock(InputCollection.class, Mockito.RETURNS_DEEP_STUBS);
    JobPrototype jobPrototype = mock(JobPrototype.class);
    when(inputCollection.getName()).thenReturn("input");
    when(inputCollection.getSource().toString()).thenReturn("source");

    assertEquals("\"source\" [label=\"input\" shape=folder];",
        dotfileWriter.formatPCollectionNodeDeclaration(inputCollection, jobPrototype));
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
  public void testFormatNodePath() {
    PCollectionImpl<?> tail = mock(PCollectionImpl.class);
    PCollectionImpl<?> head = mock(PCollectionImpl.class);
    JobPrototype jobPrototype = mock(JobPrototype.class);

    when(tail.getName()).thenReturn("tail");
    when(head.getName()).thenReturn("head");

    NodePath nodePath = new NodePath(tail);
    nodePath.close(head);

    assertEquals(
        Lists.newArrayList("\"head@" + head.hashCode() + "@" + jobPrototype.hashCode() + "\" -> \"tail@"
            + tail.hashCode() + "@" + jobPrototype.hashCode() + "\";"),
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

}
