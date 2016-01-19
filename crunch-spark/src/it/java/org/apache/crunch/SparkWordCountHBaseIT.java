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
package org.apache.crunch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.hbase.HBaseSourceTarget;
import org.apache.crunch.io.hbase.HBaseTypes;
import org.apache.crunch.test.TemporaryPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
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

public class SparkWordCountHBaseIT {

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
    public TemporaryPath tmpDir = new TemporaryPath();

    private static final byte[] COUNTS_COLFAM = Bytes.toBytes("cf");
    private static final byte[] WORD_COLFAM = Bytes.toBytes("cf");

    private HBaseTestingUtility hbaseTestUtil;

    @SuppressWarnings("serial")
    public static PTable<String, Long> wordCount(PTable<ImmutableBytesWritable, Result> words) {
        return words.parallelDo(
            new DoFn<Pair<ImmutableBytesWritable, Result>, String>() {
                @Override
                public void process(Pair<ImmutableBytesWritable, Result> row, Emitter<String> emitter) {
                    byte[] word = row.second().getValue(WORD_COLFAM, null);
                    if (word != null) {
                        emitter.emit(Bytes.toString(word));
                    }
                }
            }, words.getTypeFamily().strings()).count();

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
        run(new SparkPipeline("local", "hbaseWordCount",
            SparkWordCountHBaseIT.class, hbaseTestUtil.getConfiguration()));
    }

    @Test
    public void testWordCountCustomFormat() throws Exception {
        run(new SparkPipeline("local", "hbaseWordCountCustom",
            SparkWordCountHBaseIT.class, hbaseTestUtil.getConfiguration()), MyTableInputFormat.class);
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

        HTable inputTable = hbaseTestUtil.createTable(Bytes.toBytes(inputTableName), WORD_COLFAM);

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
        if (clazz == null) {
            source = new HBaseSourceTarget(TableName.valueOf(inputTableName), scan, scan2);
        } else {
            source = new HBaseSourceTarget(inputTableName, clazz, new Scan[]{scan, scan2});
        }

        PTable<ImmutableBytesWritable, Result> words = pipeline.read(source);
        PTable<String, Long> counts = wordCount(words);
        Map<String, Long> countMap = counts.materializeToMap();
        assertEquals(2, countMap.size());
        assertEquals(2L, countMap.get("cat").longValue());
        assertEquals(1L, countMap.get("dog").longValue());
        pipeline.done();
    }

    protected int put(HTable table, int key, String value) throws IOException {
        Put put = new Put(Bytes.toBytes(key));
        put.add(WORD_COLFAM, null, Bytes.toBytes(value));
        table.put(put);
        return key + 1;
    }

    public static class MyTableInputFormat extends MultiTableInputFormat{

        public static final AtomicBoolean CONSTRUCTED = new AtomicBoolean();

        public MyTableInputFormat(){
            CONSTRUCTED.set(true);
        }
    }

}
