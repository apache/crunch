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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;

import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.test.Tests;
import org.apache.crunch.types.avro.Avros;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;


public class UnionIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  private MRPipeline pipeline;
  private PCollection<String> words1;
  private PCollection<String> words2;

  @Before
  public void setUp() throws IOException {
    pipeline = new MRPipeline(UnionIT.class, tmpDir.getDefaultConfiguration());
    words1 = pipeline.readTextFile(tmpDir.copyResourceFileName(Tests.resource(this, "src1.txt")));
    words2 = pipeline.readTextFile(tmpDir.copyResourceFileName(Tests.resource(this, "src2.txt")));
  }

  @After
  public void tearDown() {
    pipeline.done();
  }

  @Test
  public void testUnion() throws Exception {
    IdentityFn<String> identity = IdentityFn.getInstance();
    words1 = words1.parallelDo(identity, Avros.strings());
    words2 = words2.parallelDo(identity, Avros.strings());

    PCollection<String> union = words1.union(words2);

    ImmutableMultiset<String> actual = ImmutableMultiset.copyOf(union.materialize());
    assertThat(actual.elementSet().size(), is(3));
    assertThat(actual.count("a1"), is(4));
    assertThat(actual.count("b2"), is(2));
    assertThat(actual.count("c3"), is(2));
  }

  @Test
  public void testTableUnion() throws IOException {
    PTable<String, String> words1ByFirstLetter = byFirstLetter(words1);
    PTable<String, String> words2ByFirstLetter = byFirstLetter(words2);

    PTable<String, String> union = words1ByFirstLetter.union(words2ByFirstLetter);

    ImmutableMultiset<Pair<String, String>> actual = ImmutableMultiset.copyOf(union.materialize());

    assertThat(actual.elementSet().size(), is(3));
    assertThat(actual.count(Pair.of("a", "1")), is(4));
    assertThat(actual.count(Pair.of("b", "2")), is(2));
    assertThat(actual.count(Pair.of("c", "3")), is(2));
  }

  @Test
  public void testUnionThenGroupByKey() throws IOException {
    PCollection<String> union = words1.union(words2);

    PGroupedTable<String, String> grouped = byFirstLetter(union).groupByKey();

    Map<String, String> actual = grouped.combineValues(Aggregators.STRING_CONCAT("", true))
        .materializeToMap();

    Map<String, String> expected = ImmutableMap.of("a", "1111", "b", "22", "c", "33");
    assertThat(actual, is(expected));
  }

  @Test
  public void testTableUnionThenGroupByKey() throws IOException {
    PTable<String, String> words1ByFirstLetter = byFirstLetter(words1);
    PTable<String, String> words2ByFirstLetter = byFirstLetter(words2);

    PTable<String, String> union = words1ByFirstLetter.union(words2ByFirstLetter);

    PGroupedTable<String, String> grouped = union.groupByKey();

    Map<String, String> actual = grouped.combineValues(Aggregators.STRING_CONCAT("", true))
        .materializeToMap();

    Map<String, String> expected = ImmutableMap.of("a", "1111", "b", "22", "c", "33");
    assertThat(actual, is(expected));
  }


  private static PTable<String, String> byFirstLetter(PCollection<String> values) {
    return values.parallelDo("byFirstLetter", new FirstLetterKeyFn(),
        Avros.tableOf(Avros.strings(), Avros.strings()));
  }

  private static class FirstLetterKeyFn extends DoFn<String, Pair<String, String>> {
    @Override
    public void process(String input, Emitter<Pair<String, String>> emitter) {
      if (input.length() > 1) {
        emitter.emit(Pair.of(input.substring(0, 1), input.substring(1)));
      }
    }
  }

}
