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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import junit.framework.Assert;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.text.TextFileTarget;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

public class MemPipelineUTF8IT {

  @Rule
  public TemporaryPath baseTmpDir = TemporaryPaths.create();

  private static void writeFile(String text, String filename) throws IOException {
    Files.write(text, new File(filename), Charsets.UTF_8);
  }

  @Test
  public void testText() throws Exception {

    final String infilename = baseTmpDir.getFileName("input");
    final String memOutFilename = baseTmpDir.getFileName("memPipelineOut");
    final String mrOutFilename = baseTmpDir.getFileName("mrPipelineOut");
    final String expected = "súper";

    new File(infilename).getParentFile().mkdirs();

    writeFile(expected, infilename);

    Pipeline memPipeline = MemPipeline.getInstance();
    PCollection<String> memPColl = memPipeline.readTextFile(infilename);
    Target memTarget = new TextFileTarget(memOutFilename);
    memPipeline.write(memPColl, memTarget, WriteMode.OVERWRITE);
    memPipeline.run();
    File outDir = new File(memOutFilename);
    File actualMemOut = null;
    for (File f : outDir.listFiles()) {
      String name = f.getName();
      if (name.contains("out") && name.endsWith(".txt")) {
        actualMemOut = f;
        break;
      }
    }
    String actualMemText = Files.readFirstLine(actualMemOut, Charsets.UTF_8);

    Pipeline mrPipeline = new MRPipeline(getClass());
    PCollection<String> mrPColl = mrPipeline.readTextFile(infilename);
    Target mrTarget = new TextFileTarget(mrOutFilename);
    mrPipeline.write(mrPColl, mrTarget, WriteMode.OVERWRITE);
    mrPipeline.run();
    String actualMrText = Files.readFirstLine(new File(mrOutFilename + "/part-m-00000"), Charsets.UTF_8);

    Assert.assertEquals("MR file mismatch", expected, actualMrText);
    Assert.assertEquals("Mem file mismatch", expected, actualMemText);
  }
}