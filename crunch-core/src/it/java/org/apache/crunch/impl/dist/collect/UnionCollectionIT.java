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
package org.apache.crunch.impl.dist.collect;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTableKeyValueIT;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class UnionCollectionIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  private static final Logger LOG = LoggerFactory.getLogger(UnionCollectionIT.class);

  private PTypeFamily typeFamily;
  private Pipeline pipeline;
  private PCollection<String> union;

  private ArrayList<String> EXPECTED = Lists.newArrayList("a", "a", "b", "c", "c", "d", "e");

  private Class pipelineClass;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws IOException {
    String inputFile1 = tmpDir.copyResourceFileName("set1.txt");
    String inputFile2 = tmpDir.copyResourceFileName("set2.txt");
    if (pipelineClass == null) {
      pipeline = MemPipeline.getInstance();
    } else {
      pipeline = new MRPipeline(pipelineClass, tmpDir.getDefaultConfiguration());
    }
    PCollection<String> firstCollection = pipeline.read(At.textFile(inputFile1, typeFamily.strings()));
    PCollection<String> secondCollection = pipeline.read(At.textFile(inputFile2, typeFamily.strings()));

    LOG.info("Test fixture: [ {} : {}]  First: {}, Second: {}", new Object[]{pipeline.getClass().getSimpleName()
        ,typeFamily.getClass().getSimpleName(), Lists.newArrayList(firstCollection.materialize().iterator()),
        Lists.newArrayList(secondCollection.materialize().iterator())});

    union = secondCollection.union(firstCollection);
  }

  @Parameters
  public static Collection<Object[]> data() throws IOException {
    Object[][] data = new Object[][] { { WritableTypeFamily.getInstance(), PTableKeyValueIT.class },
        { WritableTypeFamily.getInstance(), null }, { AvroTypeFamily.getInstance(), PTableKeyValueIT.class },
        { AvroTypeFamily.getInstance(), null } };
    return Arrays.asList(data);
  }

  public UnionCollectionIT(PTypeFamily typeFamily, Class pipelineClass) {
    this.typeFamily = typeFamily;
    this.pipelineClass = pipelineClass;
  }

  @Test
  public void unionMaterializeShouldNotThrowNPE() throws Exception {
    checkMaterialized(union.materialize());
    checkMaterialized(pipeline.materialize(union));
  }

  private void checkMaterialized(Iterable<String> materialized) {
    List<String> materializedValues = Lists.newArrayList(materialized.iterator());
    Collections.sort(materializedValues);
    LOG.info("Materialized union: {}", materializedValues);
    assertEquals(EXPECTED, materializedValues);
  }

  @Test
  public void unionWriteShouldNotThrowNPE() throws IOException {
    String outputPath1 = tmpDir.getFileName("output1");
    String outputPath2 = tmpDir.getFileName("output2");
    String outputPath3 = tmpDir.getFileName("output3");

    if (typeFamily == AvroTypeFamily.getInstance()) {
      union.write(To.avroFile(outputPath1));
      pipeline.write(union, To.avroFile(outputPath2));

      pipeline.run();

      checkFileContents(outputPath1);
      checkFileContents(outputPath2);

    } else {

      union.write(To.textFile(outputPath1));
      pipeline.write(union, To.textFile(outputPath2));
      pipeline.writeTextFile(union, outputPath3);

      pipeline.run();

      checkFileContents(outputPath1);
      checkFileContents(outputPath2);
      checkFileContents(outputPath3);
    }
  }

  private void checkFileContents(String filePath) throws IOException {

    List<String> fileContentValues = (typeFamily != AvroTypeFamily.getInstance())? Lists
        .newArrayList(pipeline.read(At.textFile(filePath, typeFamily.strings())).materialize().iterator()) : Lists
        .newArrayList(pipeline.read(At.avroFile(filePath, Avros.strings())).materialize().iterator());

    Collections.sort(fileContentValues);

    LOG.info("Saved Union: {}", fileContentValues);
    assertEquals(EXPECTED, fileContentValues);
  }
}
