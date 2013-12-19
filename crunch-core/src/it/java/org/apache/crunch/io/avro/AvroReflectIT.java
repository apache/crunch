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
package org.apache.crunch.io.avro;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.Person;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AvroReflectIT implements Serializable {

  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testReflection() throws IOException {
    Pipeline pipeline = new MRPipeline(AvroReflectIT.class, tmpDir.getDefaultConfiguration());
    PCollection<StringWrapper> stringWrapperCollection = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"))
        .parallelDo(new MapFn<String, StringWrapper>() {

          @Override
          public StringWrapper map(String input) {
            StringWrapper stringWrapper = new StringWrapper();
            stringWrapper.setValue(input);
            return stringWrapper;
          }
        }, Avros.reflects(StringWrapper.class));

    List<StringWrapper> stringWrappers = Lists.newArrayList(stringWrapperCollection.materialize());

    pipeline.done();

    assertEquals(Lists.newArrayList(new StringWrapper("b"), new StringWrapper("c"), new StringWrapper("a"),
        new StringWrapper("e")), stringWrappers);

  }

  // Verify that running with a combination of reflect and specific schema
  // doesn't crash
  @Test
  public void testCombinationOfReflectionAndSpecific() throws IOException {
    Assume.assumeTrue(Avros.CAN_COMBINE_SPECIFIC_AND_REFLECT_SCHEMAS);
    Pipeline pipeline = new MRPipeline(AvroReflectIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Pair<StringWrapper, Person>> hybridPairCollection = pipeline.readTextFile(
        tmpDir.copyResourceFileName("set1.txt")).parallelDo(new MapFn<String, Pair<StringWrapper, Person>>() {

      @Override
      public Pair<StringWrapper, Person> map(String input) {
        Person person = new Person();
        person.name = input;
        person.age = 42;
        person.siblingnames = Lists.<CharSequence> newArrayList(input);

        return Pair.of(new StringWrapper(input), person);
      }
    }, Avros.pairs(Avros.reflects(StringWrapper.class), Avros.records(Person.class)));

    PCollection<Pair<String, Long>> countCollection = Aggregate.count(hybridPairCollection).parallelDo(
        new MapFn<Pair<Pair<StringWrapper, Person>, Long>, Pair<String, Long>>() {

          @Override
          public Pair<String, Long> map(Pair<Pair<StringWrapper, Person>, Long> input) {
            return Pair.of(input.first().first().getValue(), input.second());
          }
        }, Avros.pairs(Avros.strings(), Avros.longs()));

    List<Pair<String, Long>> materialized = Lists.newArrayList(countCollection.materialize());
    List<Pair<String, Long>> expected = Lists.newArrayList(Pair.of("a", 1L), Pair.of("b", 1L), Pair.of("c", 1L),
        Pair.of("e", 1L));
    Collections.sort(materialized);

    assertEquals(expected, materialized);
    pipeline.done();
  }

  private static PType<String> STRING_PTYPE = Avros.derived(String.class,
      new MapFn<StringWrapper, String>() { public String map(StringWrapper in) { return in.getValue(); }},
      new MapFn<String, StringWrapper>() { public StringWrapper map(String out) { return new StringWrapper(out); }},
      Avros.reflects(StringWrapper.class));

  @Test
  public void testDerivedReflection() throws Exception {
    Pipeline pipeline = new MRPipeline(AvroReflectIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> stringWrapperCollection = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"))
        .parallelDo(IdentityFn.<String>getInstance(), STRING_PTYPE);
    List<String> strings = Lists.newArrayList(stringWrapperCollection.materialize());
    pipeline.done();
    assertEquals(Lists.newArrayList("b", "c", "a", "e"), strings);
  }

  @Test
  public void testWrappedDerivedReflection() throws Exception {
    Pipeline pipeline = new MRPipeline(AvroReflectIT.class, tmpDir.getDefaultConfiguration());
    PCollection<Pair<Long, String>> stringWrapperCollection = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"))
        .parallelDo(new MapFn<String, Pair<Long, String>>() {
          @Override
          public Pair<Long, String> map(String input) {
            return Pair.of(1L, input);
          }
        }, Avros.pairs(Avros.longs(), STRING_PTYPE));
    List<Pair<Long, String>> pairs = Lists.newArrayList(stringWrapperCollection.materialize());
    pipeline.done();
    assertEquals(pairs.size(), 4);
    assertEquals(Pair.of(1L, "a"), pairs.get(2));
  }
}
