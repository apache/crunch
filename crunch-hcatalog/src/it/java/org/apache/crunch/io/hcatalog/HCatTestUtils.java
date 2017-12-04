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
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class HCatTestUtils {

  public static class Fns {

    /**
     * Maps an HCatRecord with a Key to a pair of the Key and the value of the
     * column "foo"
     */
    public static class KeyMapPairFn extends MapFn<HCatRecord, Pair<String, Integer>> {

      private HCatSchema schema;

      public KeyMapPairFn(HCatSchema schema) {
        this.schema = schema;
      }

      @Override
      public Pair<String, Integer> map(HCatRecord input) {
        try {
          return Pair.of(input.getString("key", schema), input.getInteger("foo", schema));
        } catch (HCatException e) {
          throw new CrunchRuntimeException(e);
        }
      }
    }

    /**
     * Takes an HCatRecord and emits a Pair<Integer, String>. assumes the
     * columns in the record are "foo" (int) and "bar" (string)
     */
    public static class MapPairFn extends MapFn<HCatRecord, Pair<Integer, String>> {

      private HCatSchema schema;

      public MapPairFn(HCatSchema schema) {
        this.schema = schema;
      }

      @Override
      public Pair<Integer, String> map(HCatRecord input) {
        try {
          return Pair.of(input.getInteger("foo", schema), input.getString("bar", schema));
        } catch (HCatException e) {
          throw new CrunchRuntimeException(e);
        }
      }
    }

    /**
     * Simple MapFn that emits the input record and emits a Pair, with the first
     * element being "record". Useful for when testing group by with the value
     * being HCatRecord
     */
    public static class GroupByHCatRecordFn extends MapFn<HCatRecord, Pair<String, DefaultHCatRecord>> {

      @Override
      public Pair<String, DefaultHCatRecord> map(HCatRecord input) {
        return Pair.of("record", (DefaultHCatRecord) input);
      }
    }

    /**
     * Takes the input iterable of DefaultHCatRecords and emits Pairs that
     * contain the value of the columns "foo" and "bar"
     */
    public static class HCatRecordMapFn extends DoFn<Pair<String, Iterable<DefaultHCatRecord>>, Pair<Integer, String>> {

      private HCatSchema schema;

      public HCatRecordMapFn(HCatSchema schema) {
        this.schema = schema;
      }

      @Override
      public void process(Pair<String, Iterable<DefaultHCatRecord>> input, Emitter<Pair<Integer, String>> emitter) {
        for (final HCatRecord record : input.second()) {
          try {
            emitter.emit(Pair.of(record.getInteger("foo", schema), record.getString("bar", schema)));
          } catch (HCatException e) {
            throw new CrunchRuntimeException(e);
          }
        }
      }
    }

    /**
     * Takes a CSV line and maps it into an HCatRecord
     */
    public static class MapHCatRecordFn extends MapFn<String, HCatRecord> {

      static HCatSchema dataSchema;

      @Override
      public void initialize() {
        try {
          dataSchema = HCatOutputFormat.getTableSchema(getConfiguration());
        } catch (IOException e) {
          throw new CrunchRuntimeException(e);
        }
      }

      @Override
      public HCatRecord map(String input) {
        try {
          return getHCatRecord(input.split(","));
        } catch (HCatException e) {
          throw new CrunchRuntimeException(e);
        }
      }

      private static HCatRecord getHCatRecord(String[] csvParts) throws HCatException {
        // must be set, or all subsequent sets on HCatRecord will fail. setting
        // the size
        // initializes the initial backing array
        DefaultHCatRecord hcatRecord = new DefaultHCatRecord(dataSchema.size());

        hcatRecord.set("foo", dataSchema, Integer.parseInt(csvParts[0]));
        hcatRecord.set("bar", dataSchema, csvParts[1]);

        return hcatRecord;
      }
    }

    /**
     * Takes an iterable of HCatRecords and emits each HCatRecord (turns a
     * PTable into a PCollection)
     */
    public static class IterableToHCatRecordMapFn extends DoFn<Pair<String, Iterable<DefaultHCatRecord>>, HCatRecord> {

      @Override
      public void process(Pair<String, Iterable<DefaultHCatRecord>> input, Emitter<HCatRecord> emitter) {
        for (final HCatRecord record : input.second()) {
          emitter.emit(record);
        }
      }
    }
  }

  public static Table createUnpartitionedTable(IMetaStoreClient client, String tableName, TableType type)
      throws IOException, HiveException, TException {
    return createTable(client, "default", tableName, type, null, Collections.<FieldSchema> emptyList());
  }

  public static Table createUnpartitionedTable(IMetaStoreClient client, String tableName, TableType type,
      @Nullable Path datalocation) throws IOException, HiveException, TException {
    return createTable(client, "default", tableName, type, datalocation, Collections.<FieldSchema> emptyList());
  }

  public static Table createTable(IMetaStoreClient client, String db, String tableName, TableType type,
      @Nullable Path datalocation, List<FieldSchema> partCols) throws IOException, HiveException, TException {
    org.apache.hadoop.hive.ql.metadata.Table tbl = new org.apache.hadoop.hive.ql.metadata.Table(db, tableName);
    tbl.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
    tbl.setTableType(type);

    if (datalocation != null)
      tbl.setDataLocation(datalocation);

    FieldSchema f1 = new FieldSchema();
    f1.setName("foo");
    f1.setType("int");
    FieldSchema f2 = new FieldSchema();
    f2.setName("bar");
    f2.setType("string");

    if (partCols != null && !partCols.isEmpty())
      tbl.setPartCols(partCols);

    tbl.setFields(ImmutableList.of(f1, f2));
    tbl.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    tbl.setSerdeParam("field.delim", ",");
    tbl.setSerdeParam("serialization.format", ",");
    tbl.setInputFormatClass("org.apache.hadoop.mapred.TextInputFormat");
    tbl.setOutputFormatClass("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    client.createTable(tbl.getTTable());

    return client.getTable(db, tableName);
  }

  public static Partition createPartition(Table table, Path partLocation, List<String> partValues) {
    Partition partition = new Partition();
    partition.setDbName(table.getDbName());
    partition.setTableName(table.getTableName());
    partition.setSd(new StorageDescriptor(table.getSd()));
    partition.setValues(partValues);
    partition.getSd().setLocation(partLocation.toString());
    return partition;
  }
}
