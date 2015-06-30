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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

public class WordCountHBaseIT {

  static class StringifyFn extends MapFn<Pair<ImmutableBytesWritable, Pair<Result, Result>>, String> {
    @Override
    public String map(Pair<ImmutableBytesWritable, Pair<Result, Result>> input) {
      byte[] firstStrBytes = input.second().first().getValue(WORD_COLFAM, null);
      byte[] secondStrBytes = input.second().second().getValue(WORD_COLFAM, null);
      if (firstStrBytes != null && secondStrBytes != null) {
        return Joiner.on(',').join(new String(firstStrBytes, Charset.forName("UTF-8")),
                                   new String(secondStrBytes, Charset.forName("UTF-8")));
      }
      return "";
    }
  }

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  private static final byte[] COUNTS_COLFAM = Bytes.toBytes("cf");
  private static final byte[] WORD_COLFAM = Bytes.toBytes("cf");

  private HBaseTestingUtility hbaseTestUtil;

  @SuppressWarnings("serial")
  public static PCollection<Put> wordCount(PTable<ImmutableBytesWritable, Result> words) {
    PTable<String, Long> counts = words.parallelDo(
        new DoFn<Pair<ImmutableBytesWritable, Result>, String>() {
          @Override
          public void process(Pair<ImmutableBytesWritable, Result> row, Emitter<String> emitter) {
            byte[] word = row.second().getValue(WORD_COLFAM, null);
            if (word != null) {
              emitter.emit(Bytes.toString(word));
            }
          }
        }, words.getTypeFamily().strings()).count();

    return counts.parallelDo("convert to put", new DoFn<Pair<String, Long>, Put>() {
      @Override
      public void process(Pair<String, Long> input, Emitter<Put> emitter) {
        Put put = new Put(Bytes.toBytes(input.first()));
        put.add(COUNTS_COLFAM, null, Bytes.toBytes(input.second()));
        emitter.emit(put);
      }

    }, HBaseTypes.puts());
  }
  
  @SuppressWarnings("serial")
  public static PCollection<Delete> clearCounts(PTable<ImmutableBytesWritable, Result> counts) {
    return counts.parallelDo("convert to delete", new DoFn<Pair<ImmutableBytesWritable, Result>, Delete>() {
      @Override
      public void process(Pair<ImmutableBytesWritable, Result> input, Emitter<Delete> emitter) {
        Delete delete = new Delete(input.first().get());
        emitter.emit(delete);
      }

    }, HBaseTypes.deletes());
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = HBaseConfiguration.create(tmpDir.getDefaultConfiguration());
    hbaseTestUtil = new HBaseTestingUtility(conf);
    hbaseTestUtil.startMiniZKCluster();
    hbaseTestUtil.startMiniHBaseCluster(1, 1);
  }

  @Test
  public void testWordCount() throws Exception {
    run(new MRPipeline(WordCountHBaseIT.class, hbaseTestUtil.getConfiguration()));
  }

  @Test
  public void testWordCountCustomFormat() throws Exception {
    run(new MRPipeline(WordCountHBaseIT.class, hbaseTestUtil.getConfiguration()), MyTableInputFormat.class);
    assertTrue(MyTableInputFormat.CONSTRUCTED.get());
  }

  @After
  public void tearDown() throws Exception {
    hbaseTestUtil.shutdownMiniHBaseCluster();
    hbaseTestUtil.shutdownMiniZKCluster();
  }

  public void run(Pipeline pipeline) throws Exception {
    run(pipeline, null);
  }

  public void run(Pipeline pipeline, Class<? extends MultiTableInputFormatBase> clazz) throws Exception {

    Random rand = new Random();
    int postFix = rand.nextInt() & 0x7FFFFFFF;
    String inputTableName = "crunch_words_" + postFix;
    String outputTableName = "crunch_counts_" + postFix;
    String otherTableName = "crunch_other_" + postFix;
    String joinTableName = "crunch_join_words_" + postFix;

    HTable inputTable = hbaseTestUtil.createTable(Bytes.toBytes(inputTableName), WORD_COLFAM);
    HTable outputTable = hbaseTestUtil.createTable(Bytes.toBytes(outputTableName), COUNTS_COLFAM);
    HTable otherTable = hbaseTestUtil.createTable(Bytes.toBytes(otherTableName), COUNTS_COLFAM);

    int key = 0;
    key = put(inputTable, key, "cat");
    key = put(inputTable, key, "cat");
    key = put(inputTable, key, "dog");
    inputTable.flushCommits();

    //Setup scan using multiple scans that simply cut the rows in half.
    Scan scan = new Scan();
    scan.addFamily(WORD_COLFAM);
    byte[] cutoffPoint = Bytes.toBytes(2);
    scan.setStopRow(cutoffPoint);
    Scan scan2 = new Scan();
    scan.addFamily(WORD_COLFAM);
    scan2.setStartRow(cutoffPoint);

    HBaseSourceTarget source = null;
    if(clazz == null){
      source = new HBaseSourceTarget(inputTableName, scan, scan2);
    }else{
      source = new HBaseSourceTarget(inputTableName, clazz, new Scan[]{scan, scan2});
    }

    PTable<ImmutableBytesWritable, Result> words = pipeline.read(source);

    Map<ImmutableBytesWritable, Result> materialized = words.materializeToMap();
    assertEquals(3, materialized.size());
    PCollection<Put> puts = wordCount(words);
    pipeline.write(puts, new HBaseTarget(outputTableName));
    pipeline.write(puts, new HBaseTarget(otherTableName));
    PipelineResult res = pipeline.done();
    assertTrue(res.succeeded());

    assertIsLong(otherTable, "cat", 2);
    assertIsLong(otherTable, "dog", 1);
    assertIsLong(outputTable, "cat", 2);
    assertIsLong(outputTable, "dog", 1);

    // verify we can do joins.
    HTable joinTable = hbaseTestUtil.createTable(Bytes.toBytes(joinTableName), WORD_COLFAM);
    try {
      key = 0;
      key = put(joinTable, key, "zebra");
      key = put(joinTable, key, "donkey");
      key = put(joinTable, key, "bird");
      key = put(joinTable, key, "horse");
      joinTable.flushCommits();
    } finally {
      joinTable.close();
    }

    Scan joinScan = new Scan();
    joinScan.addFamily(WORD_COLFAM);
    PTable<ImmutableBytesWritable, Result> other = pipeline.read(FromHBase.table(joinTableName, joinScan));
    PCollection<String> joined = words.join(other).parallelDo(new StringifyFn(), Writables.strings());
    assertEquals(ImmutableSet.of("cat,zebra", "cat,donkey", "dog,bird"),
        ImmutableSet.copyOf(joined.materialize()));
    pipeline.done();

    //verify HBaseTarget supports deletes.
    Scan clearScan = new Scan();
    clearScan.addFamily(COUNTS_COLFAM);
    pipeline = new MRPipeline(WordCountHBaseIT.class, hbaseTestUtil.getConfiguration());
    HBaseSourceTarget clearSource = new HBaseSourceTarget(outputTableName, clearScan);
    PTable<ImmutableBytesWritable, Result> counts = pipeline.read(clearSource);
    pipeline.write(clearCounts(counts), new HBaseTarget(outputTableName));
    pipeline.done();

    assertDeleted(outputTable, "cat");
    assertDeleted(outputTable, "dog");
  }

  protected int put(HTable table, int key, String value) throws IOException {
    Put put = new Put(Bytes.toBytes(key));
    put.add(WORD_COLFAM, null, Bytes.toBytes(value));
    table.put(put);
    return key + 1;
  }

  protected static void assertIsLong(HTable table, String key, long i) throws IOException {
    Get get = new Get(Bytes.toBytes(key));
    get.addFamily(COUNTS_COLFAM);
    Result result = table.get(get);

    byte[] rawCount = result.getValue(COUNTS_COLFAM, null);
    assertNotNull(rawCount);
    assertEquals(i, Bytes.toLong(rawCount));
  }
  
  protected static void assertDeleted(HTable table, String key) throws IOException {
    Get get = new Get(Bytes.toBytes(key));
    get.addFamily(COUNTS_COLFAM);
    Result result = table.get(get);
    assertTrue(result.isEmpty());
  }

  public static class MyTableInputFormat extends MultiTableInputFormat{

    public static final AtomicBoolean CONSTRUCTED = new AtomicBoolean();

    public MyTableInputFormat(){
      CONSTRUCTED.set(true);
    }
  }

}
