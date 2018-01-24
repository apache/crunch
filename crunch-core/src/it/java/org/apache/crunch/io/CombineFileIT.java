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
package org.apache.crunch.io;

import com.google.common.io.Files;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.test.Tests;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CombineFileIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testCombine() throws Exception {
    final File srcFiles = tmpDir.getFile("srcs");
    File outputFiles = tmpDir.getFile("out");
    assertTrue(srcFiles.mkdir());
    File src1 = tmpDir.copyResourceFile(Tests.resource(this, "src1.txt"));
    File src2 = tmpDir.copyResourceFile(Tests.resource(this, "src2.txt"));
    Files.copy(src1, new File(srcFiles, "src1.txt"));
    Files.copy(src2, new File(srcFiles, "src2.txt"));

    MRPipeline p = new MRPipeline(CombineFileIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> in = p.readTextFile(srcFiles.getAbsolutePath());
    PCollection<Pair<String, String>> out = in.parallelDo(
            new IdentityPlusPathFn(srcFiles), Avros.pairs(Avros.strings(), Avros.strings()));
    out.write(To.textFile(outputFiles.getAbsolutePath()));
    p.done();
    assertEquals(4, outputFiles.listFiles().length);

    // verify "crunch.split.file" is being handled correctly
    FileSystem fs = FileSystem.get(tmpDir.getDefaultConfiguration());
    Path qualifiedSourcePath = fs.makeQualified(new Path(srcFiles.getAbsolutePath()));
    Iterable<Pair<String, String>> materialized = out.materialize();
    for (Pair<String, String> pair : materialized) {
      Path path = new Path(pair.first());
      String text = pair.second();
      assertEquals(qualifiedSourcePath, path.getParent());
      String fileName = path.getName();

      // make sure filename is correct for each record
      String[] parts = text.split(",");
      switch (fileName) {
        case "src1.txt":
          assertEquals("1", parts[1].substring(0, 1));
          break;
        case "src2.txt":
          assertEquals("2", parts[1].substring(0, 1));
          break;
        default:
          fail("unexpected filename: " + fileName);
      }
    }

  }

  private static class IdentityPlusPathFn extends DoFn<String, Pair<String, String>> {
    private final File srcFiles;

    public IdentityPlusPathFn(File srcFiles) {
      this.srcFiles = srcFiles;
    }

    @Override
    public void process(String input, Emitter<Pair<String, String>> emitter) {
      String filePath = getConfiguration().get("crunch.split.file");
      assertNotNull(filePath);

      emitter.emit(Pair.of(filePath, input));
    }
  }
}
