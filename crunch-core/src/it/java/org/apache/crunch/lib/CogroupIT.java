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
package org.apache.crunch.lib;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.PersonProtos.Person;
import org.apache.crunch.lib.PersonProtos.Person.Builder;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.test.Tests;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.PTypes;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


public class CogroupIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  private MRPipeline pipeline;
  private PCollection<String> lines1;
  private PCollection<String> lines2;
  private PCollection<String> lines3;
  private PCollection<String> lines4;

  @Before
  public void setUp() throws IOException {
    pipeline = new MRPipeline(CogroupIT.class, tmpDir.getDefaultConfiguration());
    lines1 = pipeline.readTextFile(tmpDir.copyResourceFileName(Tests.resource(this, "src1.txt")));
    lines2 = pipeline.readTextFile(tmpDir.copyResourceFileName(Tests.resource(this, "src2.txt")));
    lines3 = pipeline.readTextFile(tmpDir.copyResourceFileName(Tests.resource(this, "src1.txt")));
    lines4 = pipeline.readTextFile(tmpDir.copyResourceFileName(Tests.resource(this, "src2.txt")));
  }

  @After
  public void tearDown() {
    pipeline.done();
  }

  @Test
  public void testCogroupWritables() {
    runCogroup(WritableTypeFamily.getInstance());
  }

  @Test
  public void testCogroupAvro() {
    runCogroup(AvroTypeFamily.getInstance());
  }

  @Test
  public void testCogroup3Writables() {
    runCogroup3(WritableTypeFamily.getInstance());
  }

  @Test
  public void testCogroup3Avro() {
    runCogroup3(AvroTypeFamily.getInstance());
  }
  
  @Test
  public void testCogroup4Writables() {
    runCogroup4(WritableTypeFamily.getInstance());
  }

  @Test
  public void testCogroup4Avro() {
    runCogroup4(AvroTypeFamily.getInstance());
  }

  @Test
  public void testCogroupNWritables() {
    runCogroupN(WritableTypeFamily.getInstance());
  }

  @Test
  public void testCogroupNAvro() {
    runCogroupN(AvroTypeFamily.getInstance());
  }

  @Test
  public void testCogroupProtosWritables() {
      runCogroupProtos(WritableTypeFamily.getInstance());
  }

  @Test
  public void testCogroupProtosAvro() {
      runCogroupProtos(AvroTypeFamily.getInstance());
  }

  @Test
  public void testCogroupProtosPairsWritables() {
    runCogroupProtosPairs(WritableTypeFamily.getInstance());
  }

  @Test
  public void testCogroupProtosPairsAvro() {
    runCogroupProtosPairs(AvroTypeFamily.getInstance());
  }

  public void runCogroup(PTypeFamily ptf) {
    PTableType<String, String> tt = ptf.tableOf(ptf.strings(), ptf.strings());

    PTable<String, String> kv1 = lines1.parallelDo("kv1", new KeyValueSplit(), tt);
    PTable<String, String> kv2 = lines2.parallelDo("kv2", new KeyValueSplit(), tt);

    PTable<String, Pair<Collection<String>, Collection<String>>> cg = Cogroup.cogroup(kv1, kv2);

    Map<String, Pair<Collection<String>, Collection<String>>> result = cg.materializeToMap();
    Map<String, Pair<Collection<String>, Collection<String>>> actual = Maps.newHashMap();
    for (Map.Entry<String, Pair<Collection<String>, Collection<String>>> e : result.entrySet()) {
      Collection<String> one = ImmutableSet.copyOf(e.getValue().first());
      Collection<String> two = ImmutableSet.copyOf(e.getValue().second());
      actual.put(e.getKey(), Pair.of(one, two));
    }
    Map<String, Pair<Collection<String>, Collection<String>>> expected = ImmutableMap.of(
        "a", Pair.of(coll("1-1", "1-4"), coll()),
        "b", Pair.of(coll("1-2"), coll("2-1")),
        "c", Pair.of(coll("1-3"), coll("2-2", "2-3")),
        "d", Pair.of(coll(), coll("2-4"))
    );

    assertThat(actual, is(expected));
  }

  public void runCogroupProtos(PTypeFamily ptf) {
    PTableType<String, Person> tt = ptf.tableOf(ptf.strings(), PTypes.protos(Person.class, ptf));

    PTable<String, Person> kv1 = lines1.parallelDo("kv1", new GenerateProto(), tt);
    PTable<String, Person> kv2 = lines2.parallelDo("kv2", new GenerateProto(), tt);

    PTable<String, Pair<Collection<Person>, Collection<Person>>> cg = Cogroup.cogroup(kv1, kv2);

    Map<String, Pair<Collection<Person>, Collection<Person>>> result = cg.materializeToMap();

    assertThat(result.size(), is(4));
  }

  public void runCogroupProtosPairs(PTypeFamily ptf) {
    PTableType<String, Pair<String, Person>> tt = ptf.tableOf(ptf.strings(), ptf.pairs(ptf.strings(), PTypes.protos(Person.class, ptf)));

    PTable<String, Pair<String, Person>> kv1 = lines1.parallelDo("kv1", new GenerateProtoPairs(), tt);
    PTable<String, Pair<String, Person>> kv2 = lines2.parallelDo("kv2", new GenerateProtoPairs(), tt);

    PTable<String, Pair<Collection<Pair<String, Person>>, Collection<Pair<String, Person>>>> cg = Cogroup.cogroup(kv1, kv2);

    Map<String, Pair<Collection<Pair<String, Person>>, Collection<Pair<String, Person>>>> result = cg.materializeToMap();

    assertThat(result.size(), is(4));
  }

  public void runCogroup3(PTypeFamily ptf) {
    PTableType<String, String> tt = ptf.tableOf(ptf.strings(), ptf.strings());

    PTable<String, String> kv1 = lines1.parallelDo("kv1", new KeyValueSplit(), tt);
    PTable<String, String> kv2 = lines2.parallelDo("kv2", new KeyValueSplit(), tt);
    PTable<String, String> kv3 = lines3.parallelDo("kv3", new KeyValueSplit(), tt);
    
    PTable<String, Tuple3.Collect<String, String, String>> cg = Cogroup.cogroup(kv1, kv2, kv3);

    Map<String, Tuple3.Collect<String, String, String>> result = cg.materializeToMap();
    Map<String, Tuple3.Collect<String, String, String>> actual = Maps.newHashMap();
    for (Map.Entry<String, Tuple3.Collect<String, String, String>> e : result.entrySet()) {
      Collection<String> one = ImmutableSet.copyOf(e.getValue().first());
      Collection<String> two = ImmutableSet.copyOf(e.getValue().second());
      Collection<String> three = ImmutableSet.copyOf(e.getValue().third());
      actual.put(e.getKey(), new Tuple3.Collect<String, String, String>(one, two, three));
    }
    Map<String, Tuple3.Collect<String, String, String>> expected = ImmutableMap.of(
        "a", new Tuple3.Collect<String, String, String>(coll("1-1", "1-4"), coll(), coll("1-1", "1-4")),
        "b", new Tuple3.Collect<String, String, String>(coll("1-2"), coll("2-1"), coll("1-2")),
        "c", new Tuple3.Collect<String, String, String>(coll("1-3"), coll("2-2", "2-3"), coll("1-3")),
        "d", new Tuple3.Collect<String, String, String>(coll(), coll("2-4"), coll())
    );

    assertThat(actual, is(expected));
  }
  
  public void runCogroup4(PTypeFamily ptf) {
    PTableType<String, String> tt = ptf.tableOf(ptf.strings(), ptf.strings());

    PTable<String, String> kv1 = lines1.parallelDo("kv1", new KeyValueSplit(), tt);
    PTable<String, String> kv2 = lines2.parallelDo("kv2", new KeyValueSplit(), tt);
    PTable<String, String> kv3 = lines3.parallelDo("kv3", new KeyValueSplit(), tt);
    PTable<String, String> kv4 = lines4.parallelDo("kv4", new KeyValueSplit(), tt);
    
    PTable<String, Tuple4.Collect<String, String, String, String>> cg = Cogroup.cogroup(kv1, kv2, kv3, kv4);

    Map<String, Tuple4.Collect<String, String, String, String>> result = cg.materializeToMap();
    Map<String, Tuple4.Collect<String, String, String, String>> actual = Maps.newHashMap();
    for (Map.Entry<String, Tuple4.Collect<String, String, String, String>> e : result.entrySet()) {
      Collection<String> one = ImmutableSet.copyOf(e.getValue().first());
      Collection<String> two = ImmutableSet.copyOf(e.getValue().second());
      Collection<String> three = ImmutableSet.copyOf(e.getValue().third());
      Collection<String> four = ImmutableSet.copyOf(e.getValue().fourth());
      actual.put(e.getKey(), new Tuple4.Collect<String, String, String, String>(one, two, three, four));
    }
    Map<String, Tuple4.Collect<String, String, String, String>> expected = ImmutableMap.of(
        "a", new Tuple4.Collect<String, String, String, String>(coll("1-1", "1-4"), coll(), coll("1-1", "1-4"), coll()),
        "b", new Tuple4.Collect<String, String, String, String>(coll("1-2"), coll("2-1"), coll("1-2"), coll("2-1")),
        "c", new Tuple4.Collect<String, String, String, String>(coll("1-3"), coll("2-2", "2-3"), coll("1-3"), coll("2-2", "2-3")),
        "d", new Tuple4.Collect<String, String, String, String>(coll(), coll("2-4"), coll(), coll("2-4"))
    );

    assertThat(actual, is(expected));
  }

  public void runCogroupN(PTypeFamily ptf) {
    PTableType<String, String> tt = ptf.tableOf(ptf.strings(), ptf.strings());

    PTable<String, String> kv1 = lines1.parallelDo("kv1", new KeyValueSplit(), tt);
    PTable<String, String> kv2 = lines2.parallelDo("kv2", new KeyValueSplit(), tt);

    PTable<String, TupleN> cg = Cogroup.cogroup(kv1, new PTable[]{kv2});

    Map<String, TupleN> result = cg.materializeToMap();
    Map<String, TupleN> actual = Maps.newHashMap();
    for (Map.Entry<String, TupleN> e : result.entrySet()) {
      Collection<String> one = ImmutableSet.copyOf((Collection<? extends String>) e.getValue().get(0));
      Collection<String> two = ImmutableSet.copyOf((Collection<? extends String>)e.getValue().get(1));
      actual.put(e.getKey(), TupleN.of(one, two));
    }
    Map<String, TupleN> expected = ImmutableMap.of(
        "a", TupleN.of(coll("1-1", "1-4"), coll()),
        "b", TupleN.of(coll("1-2"), coll("2-1")),
        "c", TupleN.of(coll("1-3"), coll("2-2", "2-3")),
        "d", TupleN.of(coll(), coll("2-4"))
    );

    assertThat(actual, is(expected));

    PType<TupleN> tupleValueType = cg.getValueType();
    List<PType> expectedSubtypes = ImmutableList.<PType>of(
        ptf.collections(ptf.strings()),
        ptf.collections(ptf.strings()));

    assertThat(tupleValueType.getSubTypes(), is(expectedSubtypes));
  }
  
  private static class KeyValueSplit extends DoFn<String, Pair<String, String>> {
    @Override
    public void process(String input, Emitter<Pair<String, String>> emitter) {
      String[] fields = input.split(",");
      emitter.emit(Pair.of(fields[0], fields[1]));
    }
  }

  private static class GenerateProto extends DoFn<String, Pair<String, Person>> {
      @Override
      public void process(String input, Emitter<Pair<String, Person>> emitter) {
          String[] fields = input.split(",");
          String key = fields[0];
          Builder b = Person.newBuilder().setFirst("first"+key).setLast("last"+key);
          emitter.emit(Pair.of(fields[0], b.build()));
      }
  }

  private static class GenerateProtoPairs extends DoFn<String, Pair<String, Pair<String, Person>>> {
      @Override
      public void process(String input, Emitter<Pair<String, Pair<String, Person>>> emitter) {
          String[] fields = input.split(",");
          String key = fields[0];
          Builder b = Person.newBuilder().setFirst("first"+key).setLast("last"+key);
          emitter.emit(Pair.of(fields[0], Pair.of(fields[1], b.build())));
      }
  }

  private static Collection<String> coll(String... values) {
    return ImmutableSet.copyOf(values);
  }
  
}
