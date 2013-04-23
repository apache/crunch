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
package org.apache.crunch;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.crunch.io.At.sequenceFile;
import static org.apache.crunch.io.At.textFile;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class PCollectionGetSizeIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  private String emptyInputPath;
  private String nonEmptyInputPath;
  private String outputPath;

  @Before
  public void setUp() throws IOException {
    emptyInputPath = tmpDir.copyResourceFileName("emptyTextFile.txt");
    nonEmptyInputPath = tmpDir.copyResourceFileName("set1.txt");
    outputPath = tmpDir.getFileName("output");
  }

  @Test
  public void testGetSizeOfEmptyInput_MRPipeline() throws IOException {
    testCollectionGetSizeOfEmptyInput(new MRPipeline(this.getClass(), tmpDir.getDefaultConfiguration()));
  }

  @Test
  public void testGetSizeOfEmptyInput_MemPipeline() throws IOException {
    testCollectionGetSizeOfEmptyInput(MemPipeline.getInstance());
  }

  private void testCollectionGetSizeOfEmptyInput(Pipeline pipeline) throws IOException {

    assertThat(pipeline.read(textFile(emptyInputPath)).getSize(), is(0L));
  }

  @Test
  public void testMaterializeEmptyInput_MRPipeline() throws IOException {
    testMaterializeEmptyInput(new MRPipeline(this.getClass(), tmpDir.getDefaultConfiguration()));
  }

  @Test
  public void testMaterializeEmptyImput_MemPipeline() throws IOException {
    testMaterializeEmptyInput(MemPipeline.getInstance());
  }

  private void testMaterializeEmptyInput(Pipeline pipeline) throws IOException {
    assertThat(newArrayList(pipeline.readTextFile(emptyInputPath).materialize().iterator()).size(), is(0));
  }

  @Test
  public void testGetSizeOfEmptyIntermediatePCollection_MRPipeline() throws IOException {

    PCollection<String> emptyIntermediate = createPesistentEmptyIntermediate(
        new MRPipeline(this.getClass(), tmpDir.getDefaultConfiguration()));

    assertThat(emptyIntermediate.getSize(), is(0L));
  }

  @Test
  @Ignore("GetSize of a DoCollection is only an estimate based on scale factor, so we can't count on it being reported as 0")
  public void testGetSizeOfEmptyIntermediatePCollection_NoSave_MRPipeline() throws IOException {

    PCollection<String> data = new MRPipeline(this.getClass(), tmpDir.getDefaultConfiguration())
      .readTextFile(nonEmptyInputPath);

    PCollection<String> emptyPCollection = data.filter(FilterFns.<String>REJECT_ALL());

    assertThat(emptyPCollection.getSize(), is(0L));
  }

  @Test
  public void testGetSizeOfEmptyIntermediatePCollection_MemPipeline() {

    PCollection<String> emptyIntermediate = createPesistentEmptyIntermediate(MemPipeline.getInstance());

    assertThat(emptyIntermediate.getSize(), is(0L));
  }

  @Test
  public void testMaterializeOfEmptyIntermediatePCollection_MRPipeline() throws IOException {

    PCollection<String> emptyIntermediate = createPesistentEmptyIntermediate(
        new MRPipeline(this.getClass(), tmpDir.getDefaultConfiguration()));

    assertThat(newArrayList(emptyIntermediate.materialize()).size(), is(0));
  }

  @Test
  public void testMaterializeOfEmptyIntermediatePCollection_MemPipeline() {

    PCollection<String> emptyIntermediate = createPesistentEmptyIntermediate(MemPipeline.getInstance());

    assertThat(newArrayList(emptyIntermediate.materialize()).size(), is(0));
  }

  private PCollection<String> createPesistentEmptyIntermediate(Pipeline pipeline) {

    PCollection<String> data = pipeline.readTextFile(nonEmptyInputPath);

    PCollection<String> emptyPCollection = data.filter(FilterFns.<String>REJECT_ALL());

    emptyPCollection.write(sequenceFile(outputPath, strings()));

    pipeline.run();

    return pipeline.read(sequenceFile(outputPath, strings()));
  }

  @Test(expected = IllegalStateException.class)
  public void testExpectExceptionForGettingSizeOfNonExistingFile_MRPipeline() throws IOException {
    new MRPipeline(this.getClass(), tmpDir.getDefaultConfiguration()).readTextFile("non_existing.file").getSize();
  }

  @Test(expected = IllegalStateException.class)
  public void testExpectExceptionForGettingSizeOfNonExistingFile_MemPipeline() {
    MemPipeline.getInstance().readTextFile("non_existing.file").getSize();
  }
}
