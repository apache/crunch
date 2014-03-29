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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.Serializable;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AvroPathPerKeyIT extends CrunchTestSupport implements Serializable {
  @Test
  public void testOutputFilePerKey() throws Exception {
    Pipeline p = new MRPipeline(AvroPathPerKeyIT.class, tempDir.getDefaultConfiguration());
    Path outDir = tempDir.getPath("out");
    p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")))
        .parallelDo(new MapFn<String, Pair<String, String>>() {
          @Override
          public Pair<String, String> map(String input) {
            String[] p = input.split("\t");
            return Pair.of(p[0], p[1]);
          }
        }, Avros.tableOf(Avros.strings(), Avros.strings()))
        .groupByKey()
        .write(new AvroPathPerKeyTarget(outDir));
    p.done();

    Set<String> names = Sets.newHashSet();
    FileSystem fs = outDir.getFileSystem(tempDir.getDefaultConfiguration());
    for (FileStatus fstat : fs.listStatus(outDir)) {
      names.add(fstat.getPath().getName());
    }
    assertEquals(ImmutableSet.of("A", "B", "_SUCCESS"), names);

    FileStatus[] aStat = fs.listStatus(new Path(outDir, "A"));
    assertEquals(1, aStat.length);
    assertEquals("part-r-00000.avro", aStat[0].getPath().getName());

    FileStatus[] bStat = fs.listStatus(new Path(outDir, "B"));
    assertEquals(1, bStat.length);
    assertEquals("part-r-00000.avro", bStat[0].getPath().getName());
  }

  @Test
  public void testOutputFilePerKey_NothingToOutput() throws Exception {
    Pipeline p = new MRPipeline(AvroPathPerKeyIT.class, tempDir.getDefaultConfiguration());
    Path outDir = tempDir.getPath("out");

    p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")))
        .parallelDo(new MapFn<String, Pair<String, String>>() {
          @Override
          public Pair<String, String> map(String input) {
            String[] p = input.split("\t");
            return Pair.of(p[0], p[1]);
          }
        }, Avros.tableOf(Avros.strings(), Avros.strings()))
        .filter(FilterFns.<Pair<String, String>>REJECT_ALL())
        .groupByKey()
        .write(new AvroPathPerKeyTarget(outDir));
    p.done();

    FileSystem fs = outDir.getFileSystem(tempDir.getDefaultConfiguration());
    assertFalse(fs.exists(outDir));
  }

}
