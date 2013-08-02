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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.KeyValueHeap;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HFileTargetIT implements Serializable {

  private static final HBaseTestingUtility HBASE_TEST_UTILITY = new HBaseTestingUtility();
  private static final byte[] TEST_TABLE = Bytes.toBytes("test_table");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("test_family");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("count");
  private static final Path TEMP_DIR = new Path("/tmp");

  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @BeforeClass
  public static void setUpClass() throws Exception {
    // We have to use mini mapreduce cluster, because LocalJobRunner allows only a single reducer
    // (we will need it to test bulk load against multiple regions).
    HBASE_TEST_UTILITY.startMiniCluster();
    HBASE_TEST_UTILITY.startMiniMapReduceCluster();
    HBaseAdmin admin = HBASE_TEST_UTILITY.getHBaseAdmin();
    HColumnDescriptor hcol = new HColumnDescriptor(TEST_FAMILY);
    HTableDescriptor htable = new HTableDescriptor(TEST_TABLE);
    htable.addFamily(hcol);
    byte[][] splits = new byte[26][];
    for (int i = 0; i < 26; i++) {
      byte b = (byte)('a' + i);
      splits[i] = new byte[] { b };
    }
    admin.createTable(htable, splits);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    HBASE_TEST_UTILITY.shutdownMiniMapReduceCluster();
    HBASE_TEST_UTILITY.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    FileSystem fs = FileSystem.get(HBASE_TEST_UTILITY.getConfiguration());
    fs.delete(TEMP_DIR, true);
    HBASE_TEST_UTILITY.truncateTable(TEST_TABLE);
  }

  @Test
  public void testHFileTarget() throws IOException {
    Configuration conf = HBASE_TEST_UTILITY.getConfiguration();
    Pipeline pipeline = new MRPipeline(HFileTargetIT.class, conf);
    Path inputPath = copyResourceFileToHDFS("shakes.txt");
    Path outputPath = getTempPathOnHDFS("out");

    PCollection<String> shakespeare = pipeline.read(At.textFile(inputPath, Writables.strings()));
    PCollection<String> words = split(shakespeare, "\\s+");
    PTable<String,Long> wordCounts = words.count();
    PCollection<KeyValue> wordCountKvs = convertToKeyValues(wordCounts);
    pipeline.write(wordCountKvs, ToHBase.hfile(outputPath));

    PipelineResult result = pipeline.run();
    assertTrue(result.succeeded());

    FileSystem fs = FileSystem.get(conf);
    KeyValue kv = readFromHFiles(fs, outputPath, "and");
    assertEquals(427L, Bytes.toLong(kv.getValue()));
  }

  @Test
  public void testBulkLoad() throws Exception {
    Configuration conf = HBASE_TEST_UTILITY.getConfiguration();
    Pipeline pipeline = new MRPipeline(HFileTargetIT.class, conf);
    Path inputPath = copyResourceFileToHDFS("shakes.txt");
    Path outputPath = getTempPathOnHDFS("out");

    PCollection<String> shakespeare = pipeline.read(At.textFile(inputPath, Writables.strings()));
    PCollection<String> words = split(shakespeare, "\\s+");
    PTable<String,Long> wordCounts = words.count();
    PCollection<KeyValue> wordCountKvs = convertToKeyValues(wordCounts);
    HTable testTable = new HTable(HBASE_TEST_UTILITY.getConfiguration(), TEST_TABLE);
    HFileUtils.writeToHFilesForIncrementalLoad(
        wordCountKvs,
        testTable,
        outputPath);

    PipelineResult result = pipeline.run();
    assertTrue(result.succeeded());

    new LoadIncrementalHFiles(HBASE_TEST_UTILITY.getConfiguration())
        .doBulkLoad(outputPath, testTable);

    Map<String, Long> EXPECTED = ImmutableMap.<String, Long>builder()
        .put("", 1470L)
        .put("the", 620L)
        .put("and", 427L)
        .put("of", 396L)
        .put("to", 367L)
        .build();
    for (Map.Entry<String, Long> e : EXPECTED.entrySet()) {
      assertEquals((long) e.getValue(), Bytes.toLong(
          testTable.get(new Get(Bytes.toBytes(e.getKey()))).getColumnLatest(TEST_FAMILY, TEST_QUALIFIER).getValue()));
    }
  }

  private PCollection<KeyValue> convertToKeyValues(PTable<String, Long> in) {
    return in.parallelDo(new MapFn<Pair<String, Long>, KeyValue>() {
      @Override
      public KeyValue map(Pair<String, Long> input) {
        String w = input.first();
        long c = input.second();
        KeyValue kv = new KeyValue(
            Bytes.toBytes(w),
            TEST_FAMILY,
            TEST_QUALIFIER,
            Bytes.toBytes(c));
        return kv;
      }
    }, Writables.writables(KeyValue.class));
  }

  private PCollection<String> split(PCollection<String> in, final String regex) {
    return in.parallelDo(new DoFn<String, String>() {
      @Override
      public void process(String input, Emitter<String> emitter) {
        for (String w : input.split(regex)) {
          emitter.emit(w);
        }
      }
    }, Writables.strings());
  }

  /** Reads the first value on a given row from a bunch of hfiles. */
  private KeyValue readFromHFiles(FileSystem fs, Path mrOutputPath, String row) throws IOException {
    List<KeyValueScanner> scanners = Lists.newArrayList();
    KeyValue fakeKV = KeyValue.createFirstOnRow(Bytes.toBytes(row));
    for (FileStatus e : fs.listStatus(mrOutputPath)) {
      Path f = e.getPath();
      if (!f.getName().startsWith("part-")) { // filter out "_SUCCESS"
        continue;
      }
      StoreFile.Reader reader = new StoreFile.Reader(
          fs,
          f,
          new CacheConfig(fs.getConf()),
          DataBlockEncoding.NONE);
      StoreFileScanner scanner = reader.getStoreFileScanner(false, false);
      scanner.seek(fakeKV); // have to call seek of each underlying scanner, otherwise KeyValueHeap won't work
      scanners.add(scanner);
    }
    assertTrue(!scanners.isEmpty());
    KeyValueScanner kvh = new KeyValueHeap(scanners, KeyValue.COMPARATOR);
    boolean seekOk = kvh.seek(fakeKV);
    assertTrue(seekOk);
    KeyValue kv = kvh.next();
    kvh.close();
    return kv;
  }

  private Path copyResourceFileToHDFS(String resourceName) throws IOException {
    Configuration conf = HBASE_TEST_UTILITY.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path resultPath = getTempPathOnHDFS(resourceName);
    InputStream in = null;
    OutputStream out = null;
    try {
      in = Resources.getResource(resourceName).openConnection().getInputStream();
      out = fs.create(resultPath);
      IOUtils.copy(in, out);
    } finally {
      IOUtils.closeQuietly(in);
      IOUtils.closeQuietly(out);
    }
    return resultPath;
  }

  private Path getTempPathOnHDFS(String fileName) throws IOException {
    Configuration conf = HBASE_TEST_UTILITY.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path result = new Path(TEMP_DIR, fileName);
    return result.makeQualified(fs);
  }
}
