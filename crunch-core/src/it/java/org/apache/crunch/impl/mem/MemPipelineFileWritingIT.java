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
package org.apache.crunch.impl.mem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

public class MemPipelineFileWritingIT {
  @Rule
  public TemporaryPath baseTmpDir = TemporaryPaths.create();

  @Test
  public void testMemPipelineFileWriter() throws Exception {
    File tmpDir = baseTmpDir.getFile("mempipe");
    Pipeline p = MemPipeline.getInstance();
    PCollection<String> lines = MemPipeline.collectionOf("hello", "world");
    p.writeTextFile(lines, tmpDir.toString());
    p.done();
    assertTrue(tmpDir.exists());
    File[] files = tmpDir.listFiles();
    assertTrue(files != null && files.length > 0);
    for (File f : files) {
      if (!f.getName().startsWith(".")) {
        List<String> txt = Files.readLines(f, Charsets.UTF_8);
        assertEquals(ImmutableList.of("hello", "world"), txt);
      }
    }
  }
}
