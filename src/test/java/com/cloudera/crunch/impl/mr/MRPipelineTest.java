package com.cloudera.crunch.impl.mr;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.impl.mr.collect.PCollectionImpl;
import com.cloudera.crunch.io.ReadableSourceTarget;
import com.cloudera.crunch.types.avro.Avros;

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
