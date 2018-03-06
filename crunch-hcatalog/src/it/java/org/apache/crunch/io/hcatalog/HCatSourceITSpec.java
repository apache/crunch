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
package org.apache.crunch.io.hcatalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.ReadableData;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class HCatSourceITSpec extends CrunchTestSupport {

  private static IMetaStoreClient client;
  private static TemporaryPath temporaryPath;
  private static Configuration conf;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUp() throws Throwable {
    HCatTestSuiteIT.startTest();
    client = HCatTestSuiteIT.getClient();
    temporaryPath = HCatTestSuiteIT.getRootPath();
    conf = HCatTestSuiteIT.getConf();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HCatTestSuiteIT.endTest();
  }

  @Test
  public void testBasic() throws Exception {
    String tableName = testName.getMethodName();
    Path tableRootLocation = temporaryPath.getPath(tableName);
    String data = "17,josh\n29,indiana\n";
    writeDataToHdfs(data, tableRootLocation, conf);
    HCatTestUtils.createUnpartitionedTable(client, tableName, TableType.MANAGED_TABLE, tableRootLocation);

    Pipeline p = new MRPipeline(HCatSourceITSpec.class, conf);
    HCatSourceTarget src = (HCatSourceTarget) FromHCat.table(tableName);
    HCatSchema schema = src.getTableSchema(p.getConfiguration());
    PCollection<HCatRecord> records = p.read(src);
    List<Pair<Integer, String>> mat = Lists.newArrayList(
        records.parallelDo(new HCatTestUtils.Fns.MapPairFn(schema), Avros.tableOf(Avros.ints(), Avros.strings()))
            .materialize());
    p.done();
    assertEquals(ImmutableList.of(Pair.of(17, "josh"), Pair.of(29, "indiana")), mat);
  }

  @Test
  public void testReadable() throws Exception {
    String tableName = testName.getMethodName();
    Path tableRootLocation = temporaryPath.getPath(tableName);
    String data = "17,josh\n29,indiana\n";
    writeDataToHdfs(data, tableRootLocation, conf);
    HCatTestUtils.createUnpartitionedTable(client, tableName, TableType.MANAGED_TABLE, tableRootLocation);

    Pipeline p = new MRPipeline(HCatSourceITSpec.class, conf);
    HCatSourceTarget src = (HCatSourceTarget) FromHCat.table(tableName);
    HCatSchema schema = src.getTableSchema(p.getConfiguration());
    PCollection<HCatRecord> records = p.read(src);

    ReadableData<HCatRecord> readable = records.asReadable(true);
    TaskInputOutputContext mockTIOC = Mockito.mock(TaskInputOutputContext.class);
    when(mockTIOC.getConfiguration()).thenReturn(conf);
    readable.configure(conf);

    Iterator<HCatRecord> iterator = readable.read(mockTIOC).iterator();
    HCatTestUtils.Fns.MapPairFn fn = new HCatTestUtils.Fns.MapPairFn(schema);
    List<Pair<Integer, String>> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(fn.map(iterator.next()));
    }

    p.done();
    assertEquals(ImmutableList.of(Pair.of(17, "josh"), Pair.of(29, "indiana")), results);
  }

  @Test
  public void testmaterialize() throws Exception {
    String tableName = testName.getMethodName();
    Path tableRootLocation = temporaryPath.getPath(tableName);
    String data = "17,josh\n29,indiana\n";
    writeDataToHdfs(data, tableRootLocation, conf);
    HCatTestUtils.createUnpartitionedTable(client, tableName, TableType.MANAGED_TABLE, tableRootLocation);

    Pipeline p = new MRPipeline(HCatSourceITSpec.class, conf);
    HCatSourceTarget src = (HCatSourceTarget) FromHCat.table(tableName);
    HCatSchema schema = src.getTableSchema(p.getConfiguration());
    PCollection<HCatRecord> records = p.read(src);

    // force the materialize here on the HCatRecords themselves ... then
    // transform
    Iterable<HCatRecord> materialize = records.materialize();
    HCatTestUtils.Fns.MapPairFn fn = new HCatTestUtils.Fns.MapPairFn(schema);
    List<Pair<Integer, String>> results = new ArrayList<>();
    for (final HCatRecord record : materialize) {
      results.add(fn.map(record));
    }

    p.done();
    assertEquals(ImmutableList.of(Pair.of(17, "josh"), Pair.of(29, "indiana")), results);
  }

  @Test
  public void testMaterialize_partitionedTable_multiplePartitionsRequested() throws Exception {
    String tableName = testName.getMethodName();
    Path tableRootLocation = temporaryPath.getPath(tableName);
    String part1Data = "17,josh\n29,indiana\n";
    String part1Value = "1234";
    Path partition1Location = new Path(tableRootLocation, part1Value);
    String part2Data = "42,jackie\n17,ohio\n";
    String part2Value = "5678";
    Path partition2Location = new Path(tableRootLocation, part2Value);
    writeDataToHdfs(part1Data, partition1Location, conf);
    writeDataToHdfs(part2Data, partition2Location, conf);

    FieldSchema partitionSchema = new FieldSchema();
    partitionSchema.setName("timestamp");
    partitionSchema.setType("string");

    Table table = HCatTestUtils.createTable(client, "default", tableName, TableType.EXTERNAL_TABLE, tableRootLocation,
        Collections.singletonList(partitionSchema));
    client
        .add_partition(HCatTestUtils.createPartition(table, partition1Location, Collections.singletonList(part1Value)));
    client
        .add_partition(HCatTestUtils.createPartition(table, partition2Location, Collections.singletonList(part2Value)));

    Pipeline p = new MRPipeline(HCatSourceITSpec.class, conf);
    String filter = "timestamp=\"" + part1Value + "\" or timestamp=\"" + part2Value + "\"";
    // HCatSource src = new HCatSource("default", tableName, filter);
    HCatSourceTarget src = (HCatSourceTarget) FromHCat.table("default", tableName, filter);

    HCatSchema schema = src.getTableSchema(p.getConfiguration());
    PCollection<HCatRecord> records = p.read(src);
    // force the materialize here on the HCatRecords themselves ... then
    // transform
    Iterable<HCatRecord> materialize = records.materialize();
    HCatTestUtils.Fns.MapPairFn fn = new HCatTestUtils.Fns.MapPairFn(schema);
    List<Pair<Integer, String>> results = new ArrayList<>();
    for (final HCatRecord record : materialize) {
      results.add(fn.map(record));
    }

    p.done();
    assertEquals(
        ImmutableList.of(Pair.of(17, "josh"), Pair.of(29, "indiana"), Pair.of(42, "jackie"), Pair.of(17, "ohio")),
        results);
  }

  @Test
  public void testGroupBy() throws Exception {
    String tableName = testName.getMethodName();
    Path tableRootLocation = temporaryPath.getPath(tableName);
    String data = "17,josh\n29,indiana\n";
    writeDataToHdfs(data, tableRootLocation, conf);
    HCatTestUtils.createUnpartitionedTable(client, tableName, TableType.MANAGED_TABLE, tableRootLocation);

    Pipeline p = new MRPipeline(HCatSourceITSpec.class, conf);
    HCatSourceTarget src = (HCatSourceTarget) FromHCat.table(tableName);

    HCatSchema schema = src.getTableSchema(p.getConfiguration());
    PCollection<HCatRecord> records = p.read(src);
    // can't use HCatRecord here as the intermediate output is written out by
    // hadoop, and there is
    // an explicit check to ensure that the type being written out matches the
    // defined output type.
    // e.g. DefaultHCatRecord != HCatRecord, therefore an exception is thrown
    PTable<String, DefaultHCatRecord> table = records.parallelDo(new HCatTestUtils.Fns.GroupByHCatRecordFn(),
        Writables.tableOf(Writables.strings(), Writables.writables(DefaultHCatRecord.class)));

    PTable<Integer, String> finaltable = table.groupByKey().parallelDo(new HCatTestUtils.Fns.HCatRecordMapFn(schema),
        Avros.tableOf(Avros.ints(), Avros.strings()));

    List<Pair<Integer, String>> results = new ArrayList<>();
    for (final Map.Entry<Integer, String> entry : finaltable.materializeToMap().entrySet()) {
      results.add(Pair.of(entry.getKey(), entry.getValue()));
    }

    p.done();

    assertEquals(ImmutableList.of(Pair.of(17, "josh"), Pair.of(29, "indiana")), results);
  }

  @Test
  public void test_HCatRead_NonNativeTable_HBase() throws Exception {
    HBaseTestingUtility hbaseTestUtil = null;
    try {
      String db = "default";
      String hiveTable = "test";
      Configuration hbaseConf = HBaseConfiguration.create(conf);
      hbaseTestUtil = new HBaseTestingUtility(hbaseConf);
      hbaseTestUtil.startMiniZKCluster();
      hbaseTestUtil.startMiniHBaseCluster(1, 1);

      org.apache.hadoop.hbase.client.Table table = hbaseTestUtil.createTable(TableName.valueOf("test-table"), "fam");

      String key1 = "this-is-a-key";
      Put put = new Put(Bytes.toBytes(key1));
      put.addColumn("fam".getBytes(), "foo".getBytes(), "17".getBytes());
      table.put(put);
      String key2 = "this-is-a-key-too";
      Put put2 = new Put(Bytes.toBytes(key2));
      put2.addColumn("fam".getBytes(), "foo".getBytes(), "29".getBytes());
      table.put(put2);
      table.close();

      org.apache.hadoop.hive.ql.metadata.Table tbl = new org.apache.hadoop.hive.ql.metadata.Table(db, hiveTable);
      tbl.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      tbl.setTableType(TableType.EXTERNAL_TABLE);

      FieldSchema f1 = new FieldSchema();
      f1.setName("foo");
      f1.setType("int");
      FieldSchema f2 = new FieldSchema();
      f2.setName("key");
      f2.setType("string");

      tbl.setProperty("hbase.table.name", "test-table");
      tbl.setProperty("hbase.mapred.output.outputtable", "test-table");
      tbl.setProperty("storage_handler", "org.apache.hadoop.hive.hbase.HBaseStorageHandler");
      tbl.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      tbl.setFields(ImmutableList.of(f1, f2));
      tbl.setSerdeParam("hbase.columns.mapping", "fam:foo,:key");
      this.client.createTable(tbl.getTTable());

      Pipeline p = new MRPipeline(HCatSourceITSpec.class, hbaseConf);
      HCatSourceTarget src = (HCatSourceTarget) FromHCat.table(hiveTable);

      HCatSchema schema = src.getTableSchema(p.getConfiguration());
      PCollection<HCatRecord> records = p.read(src);
      List<Pair<String, Integer>> mat = Lists.newArrayList(
          records.parallelDo(new HCatTestUtils.Fns.KeyMapPairFn(schema), Avros.tableOf(Avros.strings(), Avros.ints()))
              .materialize());

      p.done();

      assertEquals(ImmutableList.of(Pair.of(key1, 17), Pair.of(key2, 29)), mat);
    } finally {
      if (hbaseTestUtil != null) {
        hbaseTestUtil.shutdownMiniHBaseCluster();
        hbaseTestUtil.shutdownMiniZKCluster();
      }
    }
  }

  // writes data to the specified location and ensures the directory exists
  // prior to writing
  private Path writeDataToHdfs(String data, Path location, Configuration conf) throws IOException {
    FileSystem fs = location.getFileSystem(conf);
    Path writeLocation = new Path(location, UUID.randomUUID().toString());
    fs.mkdirs(location);
    fs.create(writeLocation);
    ByteArrayInputStream baos = new ByteArrayInputStream(data.getBytes("UTF-8"));
    try (FSDataOutputStream fos = fs.create(writeLocation)) {
      IOUtils.copy(baos, fos);
    }

    return writeLocation;
  }
}
