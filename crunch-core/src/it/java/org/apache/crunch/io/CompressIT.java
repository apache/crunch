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

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompressIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testCompressText() throws Exception {
    String urlsFile = tmpDir.copyResourceFileName("urls.txt");
    String out = tmpDir.getFileName("out");
    MRPipeline p = new MRPipeline(CompressIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> in = p.readTextFile(urlsFile);
    in.write(Compress.gzip(To.textFile(out)));
    p.done();
    assertTrue(checkDirContainsExt(out, ".gz"));
  }

  @Test
  public void testCompressAvro() throws Exception {
    String urlsFile = tmpDir.copyResourceFileName("urls.txt");
    String out = tmpDir.getFileName("out");
    MRPipeline p = new MRPipeline(CompressIT.class, tmpDir.getDefaultConfiguration());
    PCollection<String> in = p.read(From.textFile(urlsFile, Avros.strings()));
    in.write(Compress.snappy(To.avroFile(out)));
    p.done();

    FileSystem fs = FileSystem.get(tmpDir.getDefaultConfiguration());
    FileStatus fstat = fs.getFileStatus(new Path(out, "part-m-00000.avro"));
    assertEquals(176, fstat.getLen());
  }

  private boolean checkDirContainsExt(String dir, String ext) throws Exception {
    File directory = new File(dir);
    for (File f : directory.listFiles()) {
      if (f.getName().endsWith(ext)) {
        return true;
      }
    }
    return false;
  }
}
