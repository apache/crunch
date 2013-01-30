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

import static org.junit.Assert.assertNotNull;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;



public class PTableUnionIT {

  public static class FirstLetterKeyFn extends DoFn<String, Pair<String, String>> {

    private static final long serialVersionUID = 5517897875971194220L;

    @Override
    public void process(String input, Emitter<Pair<String, String>> emitter) {
      if (input.length() > 0) {
        emitter.emit(Pair.of(input.substring(0, 1), input));
      }
    }
  }
  
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
  
  protected MRPipeline pipeline;

  @Before
  public void setUp() {
    pipeline = new MRPipeline(this.getClass(), tmpDir.getDefaultConfiguration());
  }

  @After
  public void tearDown() {
    pipeline.done();
  }

  @Test
  public void tableUnionMaterializeNPE() throws Exception {
    PCollection<String> words = pipeline.readTextFile(tmpDir.copyResourceFileName("shakes.txt"));
    PCollection<String> lorum = pipeline.readTextFile(tmpDir.copyResourceFileName("maugham.txt"));
    lorum.materialize();

    PTable<String, String> wordsByFirstLetter =
        words.parallelDo("byFirstLetter", new FirstLetterKeyFn(), Avros.tableOf(Avros.strings(), Avros.strings()));
    PTable<String, String> lorumByFirstLetter =
        lorum.parallelDo("byFirstLetter", new FirstLetterKeyFn(), Avros.tableOf(Avros.strings(), Avros.strings()));

    @SuppressWarnings("unchecked")
    PTable<String, String> union = wordsByFirstLetter.union(lorumByFirstLetter);

    assertNotNull(union.materialize().iterator().next());
  }

  @Test
  public void collectionUnionMaterializeNPE() throws Exception {
    PCollection<String> words = pipeline.readTextFile(tmpDir.copyResourceFileName("shakes.txt"));
    PCollection<String> lorum = pipeline.readTextFile(tmpDir.copyResourceFileName("maugham.txt"));
    lorum.materialize();

    IdentityFn<String> identity = IdentityFn.getInstance();
    words = words.parallelDo(identity, Avros.strings());
    lorum = lorum.parallelDo(identity, Avros.strings());

    @SuppressWarnings("unchecked")
    PCollection<String> union = words.union(lorum);

    union.materialize().iterator();
    
    assertNotNull(union.materialize().iterator().next());
  }
}
