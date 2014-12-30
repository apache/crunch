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

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.seq.SeqFileTarget;
import org.apache.crunch.io.text.TextFileTableSource;
import org.apache.crunch.io.text.TextFileTarget;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;


import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ToolRunnerIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Rule
  public TestName testName = new TestName();

  @Test
  public void textRunWithToolRunner() throws Exception {
    Configuration config = tmpDir.getDefaultConfiguration();

    String output = tmpDir.getFileName(testName.getMethodName());

    assertThat(ToolRunner.run(config, new FakeTextTool(), new String[]{tmpDir.copyResourceFileName("urls.txt"), output}), is(0));

    FileSystem fs = FileSystem.get(config);

    FileStatus fileStatus = fs.getFileStatus(new Path(output));

    assertTrue(fileStatus.isDir());

  }

  @Test
  public void textRunWithoutToolRunner() throws Exception {
    Configuration config = tmpDir.getDefaultConfiguration();

    String output = tmpDir.getFileName(testName.getMethodName());

    FakeTextTool tool = new FakeTextTool();
    tool.setConf(config);

    assertThat(tool.run(new String[]{tmpDir.copyResourceFileName("urls.txt"), output}), is(0));

    FileSystem fs = FileSystem.get(config);

    FileStatus fileStatus = fs.getFileStatus(new Path(output));

    assertTrue(fileStatus.isDir());
  }

  @Test
  public void sequenceRunWithToolRunner() throws Exception {
    Configuration config = tmpDir.getDefaultConfiguration();

    String output = tmpDir.getFileName(testName.getMethodName());

    assertThat(ToolRunner.run(config, new FakeSequenceTool(), new String[]{tmpDir.copyResourceFileName("urls.txt"), output}), is(0));

    FileSystem fs = FileSystem.get(config);

    FileStatus fileStatus = fs.getFileStatus(new Path(output));

    assertTrue(fileStatus.isDir());

  }


  @Test
  public void sequenceRunWithoutToolRunner() throws Exception {
    Configuration config = tmpDir.getDefaultConfiguration();

    String output = tmpDir.getFileName(testName.getMethodName());

    FakeSequenceTool tool = new FakeSequenceTool();
    tool.setConf(config);

    assertThat(tool.run(new String[]{tmpDir.copyResourceFileName("urls.txt"), output}), is(0));

    FileSystem fs = FileSystem.get(config);

    FileStatus fileStatus = fs.getFileStatus(new Path(output));

    assertTrue(fileStatus.isDir());
  }


  private static class FakeTextTool implements Tool, Configurable {

    private Configuration config;

    @Override
    public int run(String[] strings) throws Exception {
      String urlsFile = strings[0];
      String outFile = strings[1];
      Pipeline pipeline = new MRPipeline(ToolRunnerIT.class, getConf());

      PCollection<String> urls = pipeline.read(
          new TextFileTableSource<String, String>(urlsFile, tableOf(strings(), strings()))).values();

      pipeline.write(urls, new TextFileTarget(outFile));

      pipeline.done();

      PCollection<String> stringPCollection = pipeline.readTextFile(outFile);

      assertThat(stringPCollection.length().getValue(), is(greaterThan(0L)));

      return 0;
    }

    @Override
    public void setConf(Configuration entries) {
      config = entries;
    }

    @Override
    public Configuration getConf() {
      return config;
    }
  }

  private static class FakeSequenceTool implements Tool, Configurable {

    private Configuration config;

    @Override
    public int run(String[] strings) throws Exception {
      String urlsFile = strings[0];
      String outFile = strings[1];
      Pipeline pipeline = new MRPipeline(ToolRunnerIT.class, getConf());

      PCollection<String> urls = pipeline.read(
          new TextFileTableSource<String, String>(urlsFile, tableOf(strings(), strings()))).values();

      PType<BytesWritable> bwType = Writables.writables(BytesWritable.class);
      urls.parallelDo(new ByteConvertFn(), Writables.pairs(bwType, bwType));

      pipeline.write(urls, new SeqFileTarget(outFile));

      pipeline.done();

      PCollection<Pair<BytesWritable, BytesWritable>> stringPCollection =
          pipeline.read(From.sequenceFile(outFile, BytesWritable.class, BytesWritable.class));

      assertThat(stringPCollection.length().getValue(), is(greaterThan(0L)));

      return 0;
    }

    @Override
    public void setConf(Configuration entries) {
      config = entries;
    }

    @Override
    public Configuration getConf() {
      return config;
    }
  }

  public static class ByteConvertFn extends MapFn<String, Pair<BytesWritable, BytesWritable>> {

    @Override
    public Pair<BytesWritable, BytesWritable> map(String input) {
      BytesWritable bw = new BytesWritable(input.getBytes());
      return Pair.of(bw, bw);
    }
  }
}
