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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Iterables;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MaterializeIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testMaterializeInput_Writables() throws IOException {
    runMaterializeInput(new MRPipeline(MaterializeIT.class, tmpDir.getDefaultConfiguration()),
        WritableTypeFamily.getInstance());
  }

  @Test
  public void testMaterializeInput_Avro() throws IOException {
    runMaterializeInput(new MRPipeline(MaterializeIT.class, tmpDir.getDefaultConfiguration()),
        AvroTypeFamily.getInstance());
  }

  @Test
  public void testMaterializeInput_InMemoryWritables() throws IOException {
    runMaterializeInput(MemPipeline.getInstance(), WritableTypeFamily.getInstance());
  }

  @Test
  public void testMaterializeInput_InMemoryAvro() throws IOException {
    runMaterializeInput(MemPipeline.getInstance(), AvroTypeFamily.getInstance());
  }

  @Test
  public void testMaterializeEmptyIntermediate() throws IOException {
    runMaterializeEmptyIntermediate(
        new MRPipeline(MaterializeIT.class, tmpDir.getDefaultConfiguration()));
  }

  @Test
  public void testMaterializeEmptyIntermediate_InMemory() throws IOException {
    runMaterializeEmptyIntermediate(MemPipeline.getInstance());
  }

  @Test(expected = CrunchRuntimeException.class)
  public void testMaterializeFailure() throws IOException {
    runMaterializeWithFailure(
            new MRPipeline(MaterializeIT.class, tmpDir.getDefaultConfiguration()));
  }

  @Test
  public void testMaterializeNoFailure() throws IOException {
    Configuration conf = tmpDir.getDefaultConfiguration();
    conf.setBoolean("crunch.empty.materialize.on.failure", true);
    runMaterializeWithFailure(new MRPipeline(MaterializeIT.class, conf));
  }

  public void runMaterializeInput(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    PCollection<String> lines = pipeline.readTextFile(inputPath);
    assertEquals(expectedContent, Lists.newArrayList(lines.materialize()));
    pipeline.done();
  }

  public void runMaterializeEmptyIntermediate(Pipeline pipeline)
      throws IOException {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    PCollection<String> empty = pipeline.readTextFile(inputPath).filter(new FilterAll<String>(false));
    assertTrue(Iterables.isEmpty(empty.materialize()));
    pipeline.done();
  }

  public void runMaterializeWithFailure(Pipeline pipeline) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    PCollection<String> empty = pipeline.readTextFile(inputPath).filter(new FilterAll<String>(true));
    empty.materialize().iterator();
    pipeline.done();
  }

  static class FilterAll<T> extends FilterFn<T> {

    private final boolean throwException;

    public FilterAll(boolean throwException) {
      this.throwException = throwException;
    }

    @Override
    public boolean accept(T input) {
      if (throwException) {
        throw new RuntimeException("This is an exception");
      }
      return false;
    }
  }

  static class StringToStringWrapperPersonPairMapFn extends MapFn<String, Pair<StringWrapper, Person>> {

    @Override
    public Pair<StringWrapper, Person> map(String input) {
      Person person = new Person();
      person.name = input;
      person.age = 42;
      person.siblingnames = Lists.<CharSequence> newArrayList();
      return Pair.of(new StringWrapper(input), person);
    }

  }

  @Test
  public void testMaterializeAvroPersonAndReflectsPair_GroupedTable() throws IOException {
    Assume.assumeTrue(Avros.CAN_COMBINE_SPECIFIC_AND_REFLECT_SCHEMAS);
    Pipeline pipeline = new MRPipeline(MaterializeIT.class);
    MaterializableIterable<Pair<StringWrapper, Person>> mi = (MaterializableIterable) pipeline
        .readTextFile(tmpDir.copyResourceFileName("set1.txt"))
        .parallelDo(new StringToStringWrapperPersonPairMapFn(),
            Avros.pairs(Avros.reflects(StringWrapper.class), Avros.records(Person.class)))
        .materialize();
    
    // We just need to make sure this doesn't crash
    assertEquals(4, Iterables.size(mi));
    assertTrue(mi.getPipelineResult().succeeded());
  }
}
