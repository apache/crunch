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
package org.apache.crunch.impl.mr;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.crunch.SourceTarget;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.types.avro.Avros;
import org.junit.Before;
import org.junit.Test;

public class MRPipelineTest {

  private MRPipeline pipeline;

  @Before
  public void setUp() throws IOException {
    pipeline = spy(new MRPipeline(MRPipelineTest.class));
  }

  @Test
  public void testGetMaterializeSourceTarget_AlreadyMaterialized() {
    PCollectionImpl<String> materializedPcollection = mock(PCollectionImpl.class);
    ReadableSourceTarget<String> readableSourceTarget = mock(ReadableSourceTarget.class);
    when(materializedPcollection.getMaterializedAt()).thenReturn(readableSourceTarget);

    assertEquals(readableSourceTarget, pipeline.getMaterializeSourceTarget(materializedPcollection));
  }

  @Test
  public void testGetMaterializeSourceTarget_NotMaterialized_HasOutput() {

    PCollectionImpl<String> pcollection = mock(PCollectionImpl.class);
    ReadableSourceTarget<String> readableSourceTarget = mock(ReadableSourceTarget.class);
    when(pcollection.getPType()).thenReturn(Avros.strings());
    doReturn(readableSourceTarget).when(pipeline).createIntermediateOutput(Avros.strings());
    when(pcollection.getMaterializedAt()).thenReturn(null);

    assertEquals(readableSourceTarget, pipeline.getMaterializeSourceTarget(pcollection));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMaterializeSourceTarget_NotMaterialized_NotReadableSourceTarget() {
    PCollectionImpl<String> pcollection = mock(PCollectionImpl.class);
    SourceTarget<String> nonReadableSourceTarget = mock(SourceTarget.class);
    when(pcollection.getPType()).thenReturn(Avros.strings());
    doReturn(nonReadableSourceTarget).when(pipeline).createIntermediateOutput(Avros.strings());
    when(pcollection.getMaterializedAt()).thenReturn(null);

    pipeline.getMaterializeSourceTarget(pcollection);
  }

}
