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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HFileSourceIT implements Serializable {

  private static byte[] ROW1 = Bytes.toBytes("row1");
  private static byte[] ROW2 = Bytes.toBytes("row2");
  private static byte[] ROW3 = Bytes.toBytes("row3");
  private static byte[] FAMILY1 = Bytes.toBytes("family1");
  private static byte[] FAMILY2 = Bytes.toBytes("family2");
  private static byte[] FAMILY3 = Bytes.toBytes("family3");
  private static byte[] QUALIFIER1 = Bytes.toBytes("qualifier1");
  private static byte[] QUALIFIER2 = Bytes.toBytes("qualifier2");
  private static byte[] QUALIFIER3 = Bytes.toBytes("qualifier3");
  private static byte[] QUALIFIER4 = Bytes.toBytes("qualifier4");
  private static byte[] VALUE1 = Bytes.toBytes("value1");
  private static byte[] VALUE2 = Bytes.toBytes("value2");
  private static byte[] VALUE3 = Bytes.toBytes("value3");
  private static byte[] VALUE4 = Bytes.toBytes("value4");

  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();
  private transient Configuration conf;

  @Before
  public void setUp() {
    conf = tmpDir.getDefaultConfiguration();
  }

  @Test
  public void testHFileSource() throws IOException {
    List<KeyValue> kvs = generateKeyValues(100);
    Path inputPath = tmpDir.getPath("in");
    Path outputPath = tmpDir.getPath("out");
    writeKeyValuesToHFile(inputPath, kvs);

    Pipeline pipeline = new MRPipeline(HFileSourceIT.class, conf);
    PCollection<KeyValue> in = pipeline.read(FromHBase.hfile(inputPath));
    PCollection<String> texts = in.parallelDo(new MapFn<KeyValue, String>() {
      @Override
      public String map(KeyValue input) {
        return input.toString();
      }
    }, strings());
    texts.write(To.textFile(outputPath));
    PipelineResult result = pipeline.run();
    assertTrue(result.succeeded());

    List<String> lines = FileUtils.readLines(new File(outputPath.toString(), "part-m-00000"));
    assertEquals(kvs.size(), lines.size());
    for (int i = 0; i < kvs.size(); i++) {
      assertEquals(kvs.get(i).toString(), lines.get(i));
    }
  }

  @Test
  public void testReadHFile() throws Exception {
    List<KeyValue> kvs = generateKeyValues(100);
    assertEquals(kvs, doTestReadHFiles(kvs, new Scan()));
  }

  @Test
  public void testScanHFiles() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 0, VALUE1),
        new KeyValue(ROW1, FAMILY1, QUALIFIER2, 0, VALUE2));
    List<Result> results = doTestScanHFiles(kvs, new Scan());
    assertEquals(1, results.size());
    Result result = Iterables.getOnlyElement(results);
    assertArrayEquals(ROW1, result.getRow());
    assertEquals(2, result.rawCells().length);
    assertArrayEquals(VALUE1, CellUtil.cloneValue(result.getColumnLatestCell(FAMILY1, QUALIFIER1)));
    assertArrayEquals(VALUE2, CellUtil.cloneValue(result.getColumnLatestCell(FAMILY1, QUALIFIER2)));
  }

  @Test
  public void testScanHFiles_maxVersions() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 1, VALUE1),
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 3, VALUE3),
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 2, VALUE2));
    Scan scan = new Scan();
    scan.setMaxVersions(2);
    List<Result> results = doTestScanHFiles(kvs, scan);
    assertEquals(1, results.size());
    Result result = Iterables.getOnlyElement(results);
    List<Cell> kvs2 = result.getColumnCells(FAMILY1, QUALIFIER1);
    assertEquals(3, kvs2.size());
    assertArrayEquals(VALUE3, CellUtil.cloneValue(kvs2.get(0)));
    assertArrayEquals(VALUE2, CellUtil.cloneValue(kvs2.get(1)));
    assertArrayEquals(VALUE1, CellUtil.cloneValue(kvs2.get(2)));
  }

  @Test
  public void testScanHFiles_startStopRows() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 0, VALUE1),
        new KeyValue(ROW2, FAMILY1, QUALIFIER1, 0, VALUE1),
        new KeyValue(ROW3, FAMILY1, QUALIFIER1, 0, VALUE1));
    Scan scan = new Scan();
    scan.setStartRow(ROW2);
    scan.setStopRow(ROW3);
    List<Result> results = doTestScanHFiles(kvs, scan);
    assertEquals(1, results.size());
    Result result = Iterables.getOnlyElement(results);
    assertArrayEquals(ROW2, result.getRow());
  }

  @Test
  public void testScanHFiles_startRowIsTooSmall() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW2, FAMILY1, QUALIFIER1, 0, VALUE1),
        new KeyValue(ROW3, FAMILY1, QUALIFIER1, 0, VALUE1));
    Scan scan = new Scan();
    scan.setStartRow(ROW1);
    List<Result> results = doTestScanHFiles(kvs, scan);
    assertEquals(2, results.size());
    assertArrayEquals(ROW2, results.get(0).getRow());
    assertArrayEquals(ROW3, results.get(1).getRow());
  }

  //@Test
  public void testScanHFiles_startRowIsTooLarge() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 0, VALUE1),
        new KeyValue(ROW2, FAMILY1, QUALIFIER1, 0, VALUE1));
    Scan scan = new Scan();
    scan.setStartRow(ROW3);
    List<Result> results = doTestScanHFiles(kvs, scan);
    assertEquals(0, results.size());
  }

  @Test
  public void testScanHFiles_startRowDoesNotExist() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 0, VALUE1),
        new KeyValue(ROW3, FAMILY3, QUALIFIER3, 0, VALUE3));
    Scan scan = new Scan();
    scan.setStartRow(ROW2);
    List<Result> results = doTestScanHFiles(kvs, scan);
    assertEquals(1, results.size());
    assertArrayEquals(ROW3, results.get(0).getRow());
  }

  @Test
  public void testScanHFiles_familyMap() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 0, VALUE1),
        new KeyValue(ROW1, FAMILY2, QUALIFIER2, 0, VALUE2),
        new KeyValue(ROW1, FAMILY2, QUALIFIER3, 0, VALUE3),
        new KeyValue(ROW1, FAMILY3, QUALIFIER4, 0, VALUE4));
    Scan scan = new Scan();
    scan.addFamily(FAMILY1);
    scan.addColumn(FAMILY2, QUALIFIER2);
    List<Result> results = doTestScanHFiles(kvs, scan);
    assertEquals(1, results.size());
    Result result = Iterables.getOnlyElement(results);
    assertEquals(2, result.size());
    assertNotNull(result.getColumnLatestCell(FAMILY1, QUALIFIER1));
    assertNotNull(result.getColumnLatestCell(FAMILY2, QUALIFIER2));
  }

  @Test
  public void testScanHFiles_timeRange() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 1, VALUE1),
        new KeyValue(ROW1, FAMILY1, QUALIFIER2, 2, VALUE2),
        new KeyValue(ROW1, FAMILY1, QUALIFIER2, 3, VALUE3));
    Scan scan = new Scan();
    scan.setTimeRange(2, 3);
    List<Result> results = doTestScanHFiles(kvs, scan);
    assertEquals(1, results.size());
    Result result = Iterables.getOnlyElement(results);
    assertEquals(1, result.size());
    assertNotNull(result.getColumnLatestCell(FAMILY1, QUALIFIER2));
  }

  @Test
  public void testScanHFiles_delete() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 1, VALUE1),
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 2, VALUE2),
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 2, KeyValue.Type.Delete));
    List<Result> results = doTestScanHFiles(kvs, new Scan());
    assertEquals(1, results.size());
    assertArrayEquals(VALUE1, results.get(0).getValue(FAMILY1, QUALIFIER1));
  }

  @Test
  public void testScanHFiles_deleteColumn() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 1, VALUE1),
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 2, VALUE2),
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 2, KeyValue.Type.DeleteColumn));
    List<Result> results = doTestScanHFiles(kvs, new Scan());
    assertEquals(0, results.size());
  }

  @Test
  public void testScanHFiles_deleteFamily() throws IOException {
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 1, VALUE1),
        new KeyValue(ROW1, FAMILY1, QUALIFIER2, 2, VALUE2),
        new KeyValue(ROW1, FAMILY1, QUALIFIER3, 3, VALUE3),
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 2, KeyValue.Type.DeleteFamily));
    List<Result> results = doTestScanHFiles(kvs, new Scan());
    assertEquals(1, results.size());
    assertNull(results.get(0).getValue(FAMILY1, QUALIFIER1));
    assertNull(results.get(0).getValue(FAMILY1, QUALIFIER2));
    assertArrayEquals(VALUE3, results.get(0).getValue(FAMILY1, QUALIFIER3));
  }

  @Test
  public void testHFileSize() throws IOException {
    Path inputPath = tmpDir.getPath("in");
    List<KeyValue> kvs = ImmutableList.of(
        new KeyValue(ROW1, FAMILY1, QUALIFIER1, 1, VALUE1),
        new KeyValue(ROW1, FAMILY1, QUALIFIER2, 2, VALUE2),
        new KeyValue(ROW1, FAMILY1, QUALIFIER2, 3, VALUE3));
    writeKeyValuesToHFile(inputPath, kvs);

    FileSystem fs = FileSystem.get(conf);
    FileStatus[] fileStatuses = fs.listStatus(inputPath.getParent());
    long size = 0;
    for(FileStatus s: fileStatuses){
      size += s.getLen();
    }

    Source<KeyValue> hfile = FromHBase.hfile(inputPath);
    assertTrue(hfile.getSize(conf) >= size);
  }

  private List<Result> doTestScanHFiles(List<KeyValue> kvs, Scan scan) throws IOException {
    Path inputPath = tmpDir.getPath("in");
    writeKeyValuesToHFile(inputPath, kvs);

    Pipeline pipeline = new MRPipeline(HFileSourceIT.class, conf);
    PCollection<Result> results = HFileUtils.scanHFiles(pipeline, inputPath, scan);
    return ImmutableList.copyOf(results.materialize());
  }

  private List<KeyValue> doTestReadHFiles(List<KeyValue> kvs, Scan scan) throws IOException {
    Path inputPath = tmpDir.getPath("in");
    writeKeyValuesToHFile(inputPath, kvs);

    Pipeline pipeline = new MRPipeline(HFileSourceIT.class, conf);
    PCollection<KeyValue> results = pipeline.read(FromHBase.hfile(inputPath));
    return ImmutableList.copyOf(results.materialize());
  }

  private List<KeyValue> generateKeyValues(int count) {
    List<KeyValue> kvs = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      kvs.add(new KeyValue(
          Bytes.toBytes("row_" + i),
          Bytes.toBytes("family"),
          Bytes.toBytes("qualifier_" + i)));
    }
    Collections.sort(kvs, KeyValue.COMPARATOR);
    return kvs;
  }

  private Path writeKeyValuesToHFile(Path inputPath, List<KeyValue> kvs) throws IOException {
    HFile.Writer w = null;
    try {
      List<KeyValue> sortedKVs = Lists.newArrayList(kvs);
      Collections.sort(sortedKVs, KeyValue.COMPARATOR);
      FileSystem fs = FileSystem.get(conf);
      w = HFile.getWriterFactory(conf, new CacheConfig(conf))
          .withPath(fs, inputPath)
          .withComparator(CellComparatorImpl.COMPARATOR)
          .withFileContext(new HFileContext())
          .create();
      for (KeyValue kv : sortedKVs) {
        w.append(kv);
      }
      return inputPath;
    } finally {
      IOUtils.closeQuietly(w);
    }
  }
}
