/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.hbase.HBaseSourceTarget;
import com.cloudera.crunch.io.hbase.HBaseTarget;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.types.writable.Writables;

public class WordCountHBaseTest {
  protected static final Log LOG = LogFactory.getLog(WordCountHBaseTest.class);

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

    return counts.parallelDo("convert to put",
        new DoFn<Pair<String, Long>, Put>() {
          @Override
          public void process(Pair<String, Long> input, Emitter<Put> emitter) {
            Put put = new Put(Bytes.toBytes(input.first()));
            put.add(COUNTS_COLFAM, null,
                Bytes.toBytes(input.second()));
            emitter.emit(put);
          }

        }, Writables.writables(Put.class));
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = hbaseTestUtil.getConfiguration();
    File tmpDir = File.createTempFile("logdir", "");
    tmpDir.delete();
    tmpDir.mkdir();
    tmpDir.deleteOnExit();
    conf.set("hadoop.log.dir", tmpDir.getAbsolutePath());
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf.setInt("hbase.master.info.port", -1);
    conf.setInt("hbase.regionserver.info.port", -1);
    hbaseTestUtil.startMiniZKCluster();
    hbaseTestUtil.startMiniCluster();
    hbaseTestUtil.startMiniMapReduceCluster();
  }
  
  @Test
  public void testWordCount() throws IOException {
    run(new MRPipeline(WordCountHBaseTest.class, hbaseTestUtil.getConfiguration()));
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
      
      HTable inputTable = hbaseTestUtil.createTable(Bytes.toBytes(inputTableName),
          WORD_COLFAM);
      HTable outputTable = hbaseTestUtil.createTable(Bytes.toBytes(outputTableName),
          COUNTS_COLFAM);
  
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
