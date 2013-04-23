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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.crunch.SourceTarget;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class MRPipelineTest {
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();
  @Mock
  private PCollectionImpl<String> pcollection;
  @Mock
  private ReadableSourceTarget<String> readableSourceTarget;
  @Mock
  private SourceTarget<String> nonReadableSourceTarget;
  private MRPipeline pipeline;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    conf.set(RuntimeParameters.TMP_DIR, tempDir.getRoot().getAbsolutePath());
    pipeline = spy(new MRPipeline(MRPipelineTest.class, conf));
  }

  @Test
  public void testGetMaterializeSourceTarget_AlreadyMaterialized() {
    when(pcollection.getMaterializedAt()).thenReturn(readableSourceTarget);

    assertEquals(readableSourceTarget, pipeline.getMaterializeSourceTarget(pcollection));
  }

  @Test
  public void testGetMaterializeSourceTarget_NotMaterialized_HasOutput() {
    when(pcollection.getPType()).thenReturn(Avros.strings());
    doReturn(readableSourceTarget).when(pipeline).createIntermediateOutput(Avros.strings());
    when(pcollection.getMaterializedAt()).thenReturn(null);

    assertEquals(readableSourceTarget, pipeline.getMaterializeSourceTarget(pcollection));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMaterializeSourceTarget_NotMaterialized_NotReadableSourceTarget() {
    when(pcollection.getPType()).thenReturn(Avros.strings());
    doReturn(nonReadableSourceTarget).when(pipeline).createIntermediateOutput(Avros.strings());
    when(pcollection.getMaterializedAt()).thenReturn(null);

    pipeline.getMaterializeSourceTarget(pcollection);
  }

}
