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
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mem.collect.MemCollection;
import org.apache.crunch.impl.mem.collect.MemTable;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class MemPipelineFileReadingWritingIT {
  @Rule
  public TemporaryPath baseTmpDir = TemporaryPaths.create();
  
  private File inputFile;
  private File outputFile;
  
  
  private static final Collection<String> EXPECTED_COLLECTION = Lists.newArrayList("hello", "world");
  @SuppressWarnings("unchecked")
  private static final Collection<Pair<Integer, String>> EXPECTED_TABLE = Lists.newArrayList(
                                                        Pair.of(1, "hello"), 
                                                        Pair.of(2, "world"));
  

  @Before
  public void setUp() throws IOException {
    inputFile = baseTmpDir.getFile("test-read.seq");
    outputFile = baseTmpDir.getFile("test-write.seq");
  }

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

  private void createTestSequenceFile(final File seqFile) throws IOException {
    SequenceFile.Writer writer = null;
    writer = new Writer(FileSystem.getLocal(baseTmpDir.getDefaultConfiguration()),
              baseTmpDir.getDefaultConfiguration(), 
              new Path(seqFile.toString()), 
              IntWritable.class, Text.class);
    writer.append(new IntWritable(1), new Text("hello"));
    writer.append(new IntWritable(2), new Text("world"));
    writer.close();
  }

  @Test
  public void testMemPipelineReadSequenceFile() throws IOException {
    // set up input
    createTestSequenceFile(inputFile);

    // read from sequence file
    final PCollection<Pair<Integer, String>> readCollection = MemPipeline.getInstance().read(
      From.sequenceFile(inputFile.toString(), 
        Writables.tableOf(
          Writables.ints(), 
          Writables.strings())));

    // assert read same as written.
    assertEquals(EXPECTED_TABLE, Lists.newArrayList(readCollection.materialize()));
  }

  @Test
  public void testMemPipelineWriteSequenceFile_PCollection() throws IOException {
    // write
    PCollection<String> collection = MemPipeline.typedCollectionOf(Writables.strings(), EXPECTED_COLLECTION);
    final Target target = To.sequenceFile(outputFile.toString());
    MemPipeline.getInstance().write(collection, target);

    // read
    final SequenceFile.Reader reader = new Reader(FileSystem.getLocal(
      baseTmpDir.getDefaultConfiguration()), new Path(outputFile.toString()),
        baseTmpDir.getDefaultConfiguration());
    final List<String> actual = Lists.newArrayList();
    final NullWritable key = NullWritable.get();
    final Text value = new Text();
    while (reader.next(key, value)) {
      actual.add(value.toString());
    }
    reader.close();

    // assert read same as written
    assertEquals(EXPECTED_COLLECTION, actual);
  }

  @Test
  public void testMemPipelineWriteSequenceFile_PTable() throws IOException {
    // write
    final MemTable<Integer, String> collection = new MemTable<Integer, String>(EXPECTED_TABLE, //
        Writables.tableOf(
          Writables.ints(), 
          Writables.strings()), "test input");
    final Target target = To.sequenceFile(outputFile.toString());
    MemPipeline.getInstance().write(collection, target);

    // read
    final SequenceFile.Reader reader = new Reader(FileSystem.getLocal(baseTmpDir
        .getDefaultConfiguration()), new Path(outputFile.toString()),
        baseTmpDir.getDefaultConfiguration());
    final List<Pair<Integer, String>> actual = Lists.newArrayList();
    final IntWritable key = new IntWritable();
    final Text value = new Text();
    while (reader.next(key, value)) {
      actual.add(Pair.of(key.get(), value.toString()));
    }
    reader.close();

    // assert read same as written
    assertEquals(EXPECTED_TABLE, actual);
  }
}
