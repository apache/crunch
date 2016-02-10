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

import java.io.Serializable;

import java.nio.ByteBuffer;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.test.Person;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class EmitNullAvroIT extends CrunchTestSupport implements Serializable {
  @Test
  public void testNullableAvroPTable() throws Exception {
    // This test fails if values are not nullable
    final Pipeline p = new MRPipeline(EmitNullAvroIT.class, tempDir.getDefaultConfiguration());
    final Path outDir = tempDir.getPath("out");
    final PCollection<String> input = p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")));

    input.parallelDo(new MapFn<String, Pair<String, Person>>() {
      @Override
      public Pair<String, Person> map(final String input) {
        return new Pair<String, Person>("first name", null);
      }
    }, Avros.tableOf(Avros.strings(), Avros.records(Person.class)))
        .write(new AvroFileTarget(outDir), Target.WriteMode.APPEND);

    p.done();
  }

  @Test
  public void testNullableAvroPTable_ByteBuffer() throws Exception {
    final Pipeline p = new MRPipeline(EmitNullAvroIT.class, tempDir.getDefaultConfiguration());
    final Path outDir = tempDir.getPath("out");
    final PCollection<String> input = p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")));

    input.parallelDo(new MapFn<String, Pair<String, ByteBuffer>>() {
      @Override
      public Pair<String, ByteBuffer> map(final String input) {
        return new Pair<String, ByteBuffer>("first name", null);
      }
    }, Avros.tableOf(Avros.strings(), Avros.bytes()))
        .groupByKey()
        .write(new AvroFileTarget(outDir), Target.WriteMode.APPEND);

    p.done();
  }
}