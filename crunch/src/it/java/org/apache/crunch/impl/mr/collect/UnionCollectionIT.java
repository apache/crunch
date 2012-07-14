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
package org.apache.crunch.impl.mr.collect;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTableKeyValueIT;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.To;
import org.apache.crunch.test.FileHelper;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(value = Parameterized.class)
public class UnionCollectionIT {

  private static final Log LOG = LogFactory.getLog(UnionCollectionIT.class);

  private PTypeFamily typeFamily;
  private Pipeline pipeline;
  private PCollection<String> union;

  private ArrayList<String> EXPECTED = Lists.newArrayList("a", "a", "b", "c", "c", "d", "e");

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws IOException {
    String inputFile1 = FileHelper.createTempCopyOf("set1.txt");
    String inputFile2 = FileHelper.createTempCopyOf("set2.txt");

    PCollection<String> firstCollection = pipeline.read(At.textFile(inputFile1, typeFamily.strings()));
    PCollection<String> secondCollection = pipeline.read(At.textFile(inputFile2, typeFamily.strings()));

    LOG.info("Test fixture: [" + pipeline.getClass().getSimpleName() + " : " + typeFamily.getClass().getSimpleName()
        + "]  First: " + Lists.newArrayList(firstCollection.materialize().iterator()) + ", Second: "
        + Lists.newArrayList(secondCollection.materialize().iterator()));

    union = secondCollection.union(firstCollection);
  }

  @After
  public void tearDown() {
    pipeline.done();
  }

  @Parameters
  public static Collection<Object[]> data() throws IOException {
    Object[][] data = new Object[][] { { WritableTypeFamily.getInstance(), new MRPipeline(PTableKeyValueIT.class) },
        { WritableTypeFamily.getInstance(), MemPipeline.getInstance() },
        { AvroTypeFamily.getInstance(), new MRPipeline(PTableKeyValueIT.class) },
        { AvroTypeFamily.getInstance(), MemPipeline.getInstance() } };
    return Arrays.asList(data);
  }

  public UnionCollectionIT(PTypeFamily typeFamily, Pipeline pipeline) {
    this.typeFamily = typeFamily;
    this.pipeline = pipeline;
  }

  @Test
  public void unionMaterializeShouldNotThrowNPE() {
    checkMaterialized(union.materialize());
    checkMaterialized(pipeline.materialize(union));
  }

  private void checkMaterialized(Iterable<String> materialized) {

    List<String> materializedValues = Lists.newArrayList(materialized.iterator());
    Collections.sort(materializedValues);
    LOG.info("Materialized union: " + materializedValues);

    assertEquals(EXPECTED, materializedValues);
  }

  @Test
  public void unionWriteShouldNotThrowNPE() throws IOException {

    File outputPath1 = FileHelper.createOutputPath();
    File outputPath2 = FileHelper.createOutputPath();
    File outputPath3 = FileHelper.createOutputPath();

    if (typeFamily == AvroTypeFamily.getInstance()) {
      union.write(To.avroFile(outputPath1.getAbsolutePath()));
      pipeline.write(union, To.avroFile(outputPath2.getAbsolutePath()));

      pipeline.run();

      checkFileContents(outputPath1.getAbsolutePath());
      checkFileContents(outputPath2.getAbsolutePath());

    } else {

      union.write(To.textFile(outputPath1.getAbsolutePath()));
      pipeline.write(union, To.textFile(outputPath2.getAbsolutePath()));
      pipeline.writeTextFile(union, outputPath3.getAbsolutePath());

      pipeline.run();

      checkFileContents(outputPath1.getAbsolutePath());
      checkFileContents(outputPath2.getAbsolutePath());
      checkFileContents(outputPath3.getAbsolutePath());
    }

  }

  private void checkFileContents(String filePath) throws IOException {

    List<String> fileContentValues = (typeFamily != AvroTypeFamily.getInstance() || !(pipeline instanceof MRPipeline)) ? Lists
        .newArrayList(pipeline.read(At.textFile(filePath, typeFamily.strings())).materialize().iterator()) : Lists
        .newArrayList(pipeline.read(At.avroFile(filePath, Avros.strings())).materialize().iterator());

    Collections.sort(fileContentValues);

    LOG.info("Saved Union: " + fileContentValues);
    assertEquals(EXPECTED, fileContentValues);
  }
}
