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

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.CrunchHCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

public class HCatTarget implements MapReduceTarget {

  private static final PType<HCatRecord> PTYPE = Writables.writables(HCatRecord.class);
  private static final PType<DefaultHCatRecord> DEFAULT_PTYPE = Writables.writables(DefaultHCatRecord.class);

  private final OutputJobInfo info;
  private final FormatBundle bundle = FormatBundle.forOutput(CrunchHCatOutputFormat.class);
  private Table hiveTableCached;

  /**
   * Constructs a new instance to write to the provided hive {@code table} name.
   * Writes to the "default" database.
   * 
   * Note: if the destination table is partitioned, this constructor should not
   * be used. It will only be usable by unpartitioned tables
   *
   * @param table
   *          the hive table to write to
   */
  public HCatTarget(String table) {
    this(null, table, null);
  }

  /**
   * Constructs a new instance to write to the provided hive {@code table} name,
   * using the provided {@code database}. If null, uses "default" database.
   *
   * Note: if the destination table is partitioned, this constructor should not
   * be used. It will only be usable by unpartitioned tables
   *
   * @param database
   *          the hive database to use for table namespacing
   * @param table
   *          the hive table to write to
   */
  public HCatTarget(@Nullable String database, String table) {
    this(database, table, null);
  }

  /**
   * Constructs a new instance to write to the provided hive {@code table} name
   * and {@code partitionValues}. Writes to the "default" database.
   * 
   * Note: partitionValues will be assembled into a single directory path.
   *
   * For example, if the partition values are:
   *
   * <pre>
   * [year, 2017], 
   * [month,11], 
   * [day, 10]
   *
   * The constructed directory path will be
   * "[dataLocationRoot]/year=2017/month=11/day=10"
   * </pre>
   *
   * @param table
   *          the hive table to write to
   * @param partitionValues
   *          the partition within the table it should be written
   */
  public HCatTarget(String table, Map<String, String> partitionValues) {
    this(null, table, partitionValues);
  }

  /**
   * Constructs a new instance to write to the provided {@code database},
   * {@code table}, and to the specified {@code partitionValues}. If
   * {@code database} isn't specified, the "default" database is used
   *
   * Note: partitionValues will be assembled into a single directory path.
   *
   * For example, if the partition values are:
   *
   * <pre>
   * [year, 2017], 
   * [month,11], 
   * [day, 10]
   *
   * The constructed directory path will be
   * "[dataLocationRoot]/year=2017/month=11/day=10"
   * </pre>
   * 
   * @param database
   *          the hive database to use for table namespacing
   * @param table
   *          the hive table to write to
   * @param partitionValues
   *          the partition within the table it should be written
   */
  public HCatTarget(@Nullable String database, String table, @Nullable Map<String, String> partitionValues) {
    this.info = OutputJobInfo.create(database, table, partitionValues);
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    if (Strings.isNullOrEmpty(name)) {
      throw new AssertionError("Named output wasn't generated. This shouldn't happen");
    }
    CrunchOutputs.addNamedOutput(job, name, bundle, NullWritable.class, HCatRecord.class);

    try {
      CrunchHCatOutputFormat.setOutput(job, info);

      // set the schema into config. this would be necessary if any downstream
      // tasks need the schema translated between a format (e.g. avro) and
      // HCatRecord for the destination table
      Table table = getHiveTable(job.getConfiguration());
      CrunchHCatOutputFormat.setSchema(job, HCatUtil.extractSchema(table));
    } catch (TException | IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  @Override
  public Target outputConf(String key, String value) {
    bundle.set(key, value);
    return this;
  }

  @Override
  public boolean handleExisting(WriteMode writeMode, long lastModifiedAt, Configuration conf) {
    return writeMode == WriteMode.DEFAULT;
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (!acceptType(ptype)) {
      return false;
    }

    handler.configure(this, ptype);
    return true;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter(PType<?> ptype) {
    return ptype.getConverter();
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (acceptType(ptype))
      return (SourceTarget<T>) new HCatSourceTarget(info.getDatabaseName(), info.getTableName());

    return null;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
            .append("database", info.getDatabaseName())
            .append("table", info.getTableName())
            .append("partition", info.getPartitionValues())
            .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(info.getDatabaseName(), info.getTableName(), info.getPartitionValues());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }

    HCatTarget that = (HCatTarget) o;
    return Objects.equal(this.info.getDatabaseName(), that.info.getDatabaseName())
        && Objects.equal(this.info.getTableName(), that.info.getTableName())
        && Objects.equal(this.info.getPartitionValues(), that.info.getPartitionValues());
  }

  private boolean acceptType(PType<?> ptype) {
    return Objects.equal(ptype, PTYPE) || Objects.equal(ptype, DEFAULT_PTYPE);
  }

  private Table getHiveTable(Configuration conf) throws IOException, TException {
    if (hiveTableCached != null) {
      return hiveTableCached;
    }

    IMetaStoreClient hiveMetastoreClient = HCatUtil.getHiveMetastoreClient(new HiveConf(conf, HCatTarget.class));
    hiveTableCached = HCatUtil.getTable(hiveMetastoreClient, info.getDatabaseName(), info.getTableName());
    return hiveTableCached;
  }
}