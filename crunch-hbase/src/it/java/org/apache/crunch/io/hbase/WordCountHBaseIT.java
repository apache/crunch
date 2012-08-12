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
package org.apache.crunch.io.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.hbase.HBaseSourceTarget;
import org.apache.crunch.io.hbase.HBaseTarget;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.io.ByteStreams;

public class WordCountHBaseIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  private static final byte[] COUNTS_COLFAM = Bytes.toBytes("cf");
  private static final byte[] WORD_COLFAM = Bytes.toBytes("cf");

  private HBaseTestingUtility hbaseTestUtil = new HBaseTestingUtility();

  @SuppressWarnings("serial")
  public static PCollection<Put> wordCount(PTable<ImmutableBytesWritable, Result> words) {
    PTable<String, Long> counts = Aggregate.count(words.parallelDo(
        new DoFn<Pair<ImmutableBytesWritable, Result>, String>() {
          @Override
          public void process(Pair<ImmutableBytesWritable, Result> row, Emitter<String> emitter) {
            byte[] word = row.second().getValue(WORD_COLFAM, null);
            if (word != null) {
              emitter.emit(Bytes.toString(word));
            }
          }
        }, words.getTypeFamily().strings()));

    return counts.parallelDo("convert to put", new DoFn<Pair<String, Long>, Put>() {
      @Override
      public void process(Pair<String, Long> input, Emitter<Put> emitter) {
        Put put = new Put(Bytes.toBytes(input.first()));
        put.add(COUNTS_COLFAM, null, Bytes.toBytes(input.second()));
        emitter.emit(put);
      }

    }, Writables.writables(Put.class));
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = hbaseTestUtil.getConfiguration();
    conf.set("hadoop.log.dir", tmpDir.getFileName("logs"));
    conf.set("hadoop.tmp.dir", tmpDir.getFileName("hadoop-tmp"));
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf.setInt("hbase.master.info.port", -1);
    conf.setInt("hbase.regionserver.info.port", -1);
    
    // Workaround for HBASE-5711, we need to set config value dfs.datanode.data.dir.perm
    // equal to the permissions of the temp dirs on the filesystem. These temp dirs were
    // probably created using this process' umask. So we guess the temp dir permissions as
    // 0777 & ~umask, and use that to set the config value.
    try {
      Process process = Runtime.getRuntime().exec("/bin/sh -c umask");
      BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
      int rc = process.waitFor();
      if(rc == 0) {
        String umask = br.readLine();

        int umaskBits = Integer.parseInt(umask, 8);
        int permBits = 0777 & ~umaskBits;
        String perms = Integer.toString(permBits, 8);

        conf.set("dfs.datanode.data.dir.perm", perms);
      }
    } catch (Exception e) {
      // ignore errors, we might not be running on POSIX, or "sh" might not be on the path
    }

    hbaseTestUtil.startMiniZKCluster();
    hbaseTestUtil.startMiniCluster();
    hbaseTestUtil.startMiniMapReduceCluster(1);

    // For Hadoop-2.0.0, we have to do a bit more work.
    if (TaskAttemptContext.class.isInterface()) {
      conf = hbaseTestUtil.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      Path tmpPath = new Path("target", "WordCountHBaseTest-tmpDir");
      FileSystem localFS = FileSystem.getLocal(conf);
      for (FileStatus jarFile : localFS.listStatus(new Path("target/lib/"))) {
        Path target = new Path(tmpPath, jarFile.getPath().getName());
        fs.copyFromLocalFile(jarFile.getPath(), target);
        DistributedCache.addFileToClassPath(target, conf, fs);
      }

      // Create a programmatic container for this jar.
      JarOutputStream jos = new JarOutputStream(new FileOutputStream("WordCountHBaseIT.jar"));
      File baseDir = new File("target/test-classes");
      String prefix = "org/apache/crunch/io/hbase/";
      jarUp(jos, baseDir, prefix + "WordCountHBaseIT.class");
      jarUp(jos, baseDir, prefix + "WordCountHBaseIT$1.class");
      jarUp(jos, baseDir, prefix + "WordCountHBaseIT$2.class");
      jos.close();

      Path target = new Path(tmpPath, "WordCountHBaseIT.jar");
      fs.copyFromLocalFile(true, new Path("WordCountHBaseIT.jar"), target);
      DistributedCache.addFileToClassPath(target, conf, fs);
    }
  }

  private void jarUp(JarOutputStream jos, File baseDir, String classDir) throws IOException {
    File file = new File(baseDir, classDir);
    JarEntry e = new JarEntry(classDir);
    e.setTime(file.lastModified());
    jos.putNextEntry(e);
    ByteStreams.copy(new FileInputStream(file), jos);
    jos.closeEntry();
  }

  @Test
  public void testWordCount() throws IOException {
    run(new MRPipeline(WordCountHBaseIT.class, hbaseTestUtil.getConfiguration()));
  }

  @After
  public void tearDown() throws Exception {
    hbaseTestUtil.shutdownMiniMapReduceCluster();
    hbaseTestUtil.shutdownMiniCluster();
    hbaseTestUtil.shutdownMiniZKCluster();
  }

  public void run(Pipeline pipeline) throws IOException {

    Random rand = new Random();
    int postFix = Math.abs(rand.nextInt());
    String inputTableName = "crunch_words_" + postFix;
    String outputTableName = "crunch_counts_" + postFix;

    try {

      HTable inputTable = hbaseTestUtil.createTable(Bytes.toBytes(inputTableName), WORD_COLFAM);
      HTable outputTable = hbaseTestUtil.createTable(Bytes.toBytes(outputTableName), COUNTS_COLFAM);

      int key = 0;
      key = put(inputTable, key, "cat");
      key = put(inputTable, key, "cat");
      key = put(inputTable, key, "dog");
      Scan scan = new Scan();
      scan.addColumn(WORD_COLFAM, null);
      HBaseSourceTarget source = new HBaseSourceTarget(inputTableName, scan);
      PTable<ImmutableBytesWritable, Result> shakespeare = pipeline.read(source);
      pipeline.write(wordCount(shakespeare), new HBaseTarget(outputTableName));
      pipeline.done();

      assertIsLong(outputTable, "cat", 2);
      assertIsLong(outputTable, "dog", 1);
    } finally {
      // not quite sure...
    }
  }

  protected int put(HTable table, int key, String value) throws IOException {
    Put put = new Put(Bytes.toBytes(key));
    put.add(WORD_COLFAM, null, Bytes.toBytes(value));
    table.put(put);
    return key + 1;
  }

  protected void assertIsLong(HTable table, String key, long i) throws IOException {
    Get get = new Get(Bytes.toBytes(key));
    get.addColumn(COUNTS_COLFAM, null);
    Result result = table.get(get);

    byte[] rawCount = result.getValue(COUNTS_COLFAM, null);
    assertTrue(rawCount != null);
    assertEquals(new Long(i), new Long(Bytes.toLong(rawCount)));
  }
}
