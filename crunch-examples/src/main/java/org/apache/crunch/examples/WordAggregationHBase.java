/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.examples;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.hbase.HBaseSourceTarget;
import org.apache.crunch.io.hbase.HBaseTarget;
import org.apache.crunch.io.hbase.HBaseTypes;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * You need to have a HBase instance running. Required dependencies : hbase /!\
 * The version should be your version of hbase. <dependency>
 * <groupId>org.apache.hbase</groupId> <artifactId>hbase</artifactId>
 * <version>...</version> </dependency>
 */
@SuppressWarnings("serial")
public class WordAggregationHBase extends Configured implements Tool, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(WordAggregationHBase.class);

  // Configuration parameters. Here configured for a hbase instance running
  // locally
  private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
  private static final String hbaseZookeeperQuorum = "localhost";
  private static final String hbaseZookeeperClientPort = "2181";

  // HBase parameters
  private static final String TABLE_SOURCE = "list";
  private static final String TABLE_TARGET = "aggregation";

  private final byte[] COLUMN_FAMILY_SOURCE = Bytes.toBytes("content");
  private final byte[] COLUMN_QUALIFIER_SOURCE_PLAY = Bytes.toBytes("play");
  private final byte[] COLUMN_QUALIFIER_SOURCE_QUOTE = Bytes.toBytes("quote");

  private final byte[] COLUMN_FAMILY_TARGET = Bytes.toBytes("aggregation");
  private final byte[] COLUMN_QUALIFIER_TARGET_TEXT = Bytes.toBytes("text");

  @Override
  public int run(String[] args) throws Exception {
    // We create the test rows first
    String type1 = "romeo and juliet";
    String type2 = "macbeth";

    String quote1 = "That which we call a rose By any other word would smell as sweet";
    String quote2 = "But, soft! what light through yonder window breaks? It is the east, and Juliet is the sun.";
    String quote3 = "But first, let me tell ye, if you should leadher in a fool's paradise, as they say,";
    String quote4 = "Fair is foul, and foul is fair";
    String quote5 = "But screw your courage to the sticking-place, And we'll not fail.";

    String[] character = { "juliet", "romeo", "nurse", "witch", "macbeth" };
    String[] type = { type1, type1, type1, type2, type2 };
    String[] quote = { quote1, quote2, quote3, quote4, quote5 };

    List<Put> putList = createPuts(Arrays.asList(character), Arrays.asList(type), Arrays.asList(quote));

    // We create the tables and fill the source
    Configuration configuration = getConf();

    createTable(configuration, TABLE_SOURCE, Bytes.toString(COLUMN_FAMILY_SOURCE));
    createTable(configuration, TABLE_TARGET, Bytes.toString(COLUMN_FAMILY_TARGET));

    putInHbase(putList, configuration);

    // We create the pipeline which will handle most of the job.
    Pipeline pipeline = new MRPipeline(WordAggregationHBase.class, HBaseConfiguration.create());

    // The scan which will retrieve the data from the source in hbase.
    Scan scan = new Scan();
    scan.addColumn(COLUMN_FAMILY_SOURCE, COLUMN_QUALIFIER_SOURCE_PLAY);
    scan.addColumn(COLUMN_FAMILY_SOURCE, COLUMN_QUALIFIER_SOURCE_QUOTE);

    // Our hbase source
    HBaseSourceTarget source = new HBaseSourceTarget(TABLE_SOURCE, scan);

    // Our source, in a format which can be use by crunch
    PTable<ImmutableBytesWritable, Result> rawText = pipeline.read(source);

    // We process the data from the source HTable then concatenate all data
    // with the same rowkey
    PTable<String, String> textExtracted = extractText(rawText);
    PTable<String, String> result = textExtracted.groupByKey()
        .combineValues(Aggregators.STRING_CONCAT(" ",  true));

    // We create the collection of puts from the concatenated datas
    PCollection<Put> resultPut = createPut(result);

    // We write the puts in hbase, in the target table
    pipeline.write(resultPut, new HBaseTarget(TABLE_TARGET));

    pipeline.done();
    return 0;
  }

  /**
   * Put the puts in HBase
   * 
   * @param putList the puts
   * @param conf the hbase configuration
   * @throws IOException
   */
  private static void putInHbase(List<Put> putList, Configuration conf) throws IOException {
    HTable htable = new HTable(conf, TABLE_SOURCE);
    try {
      htable.put(putList);
    } finally {
      htable.close();
    }
  }

  /**
   * Create the table if they don't exist
   * 
   * @param conf the hbase configuration
   * @param htableName the table name
   * @param families the column family names
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  private static void createTable(Configuration conf, String htableName, String... families) throws MasterNotRunningException, ZooKeeperConnectionException,
      IOException {
    HBaseAdmin hbase = new HBaseAdmin(conf);
    try {
      if (!hbase.tableExists(htableName)) {
        HTableDescriptor desc = new HTableDescriptor(htableName);
        for (String s : families) {
          HColumnDescriptor meta = new HColumnDescriptor(s);
          desc.addFamily(meta);
        }
        hbase.createTable(desc);
      }
    } finally {
      hbase.close();
    }
  }

  /**
   * Create a list of puts
   * 
   * @param character the rowkey
   * @param play the play (in column COLUMN_QUALIFIER_SOURCE_PLAY)
   * @param quote the quote (in column COLUMN_QUALIFIER_SOURCE_QUOTE)
   */
  private List<Put> createPuts(List<String> character, List<String> play, List<String> quote) {
    List<Put> list = Lists.newArrayList();
    if (character.size() != play.size() || quote.size() != play.size()) {
      LOG.error("Every list should have the same number of elements");
      throw new IllegalArgumentException("Every list should have the same number of elements");
    }
    for (int i = 0; i < character.size(); i++) {
      Put put = new Put(Bytes.toBytes(character.get(i)));
      put.add(COLUMN_FAMILY_SOURCE, COLUMN_QUALIFIER_SOURCE_PLAY, Bytes.toBytes(play.get(i)));
      put.add(COLUMN_FAMILY_SOURCE, COLUMN_QUALIFIER_SOURCE_QUOTE, Bytes.toBytes(quote.get(i)));
      list.add(put);
    }
    return list;
  }

  /**
   * Extract information from hbase
   *
   * @param words the source from hbase
   * @return a {@code PTable} composed of the type of the input as key
   *         and its def as value
   */
  public PTable<String, String> extractText(PTable<ImmutableBytesWritable, Result> words) {
    return words.parallelDo("Extract text", new DoFn<Pair<ImmutableBytesWritable, Result>, Pair<String, String>>() {
      @Override
      public void process(Pair<ImmutableBytesWritable, Result> row, Emitter<Pair<String, String>> emitter) {
        byte[] type = row.second().getValue(COLUMN_FAMILY_SOURCE, COLUMN_QUALIFIER_SOURCE_PLAY);
        byte[] def = row.second().getValue(COLUMN_FAMILY_SOURCE, COLUMN_QUALIFIER_SOURCE_QUOTE);
        if (type != null && def != null) {
          emitter.emit(new Pair<String, String>(Bytes.toString(type), Bytes.toString(def)));
        }
      }
    }, Writables.tableOf(Writables.strings(), Writables.strings()));
  }

  /**
   * Create puts in order to insert them in hbase.
   * 
   * @param extractedText
   *            a PTable which contain the data in order to create the puts:
   *            keys of the PTable are rowkeys for the puts, values are the
   *            values for hbase.
   * @return a PCollection formed by the puts.
   */
  public PCollection<Put> createPut(PTable<String, String> extractedText) {
    return extractedText.parallelDo("Convert to puts", new DoFn<Pair<String, String>, Put>() {
      @Override
      public void process(Pair<String, String> input, Emitter<Put> emitter) {
        Put put = new Put(Bytes.toBytes(input.first()));
        put.add(COLUMN_FAMILY_TARGET, COLUMN_QUALIFIER_TARGET_TEXT, Bytes.toBytes(input.second()));
        emitter.emit(put);
      }
    }, HBaseTypes.puts());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // Configuration hbase
    conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
    conf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);
    ToolRunner.run(conf, new WordAggregationHBase(), args);
  }
}
