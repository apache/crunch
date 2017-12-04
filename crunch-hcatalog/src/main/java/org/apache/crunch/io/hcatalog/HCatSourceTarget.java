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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.impl.mr.run.CrunchMapper;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.io.SourceTargetHelper;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.hive.hcatalog.mapreduce.PartInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class HCatSourceTarget extends HCatTarget implements ReadableSourceTarget<HCatRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HCatSourceTarget.class);
  private static final PType<HCatRecord> PTYPE = Writables.writables(HCatRecord.class);
  private Configuration hcatConf;

  private final FormatBundle<HCatInputFormat> bundle = FormatBundle.forInput(HCatInputFormat.class);
  private final String database;
  private final String table;
  private final String filter;
  private Table hiveTableCached;

  // Default guess at the size of the data to materialize
  private static final long DEFAULT_ESTIMATE = 1024 * 1024 * 1024;

  /**
   * Creates a new instance to read from the specified {@code table} and the
   * {@link org.apache.hadoop.hive.metastore.MetaStoreUtils#DEFAULT_DATABASE_NAME
   * default} database
   *
   * @param table
   * @throw IllegalArgumentException if table is null or empty
   */
  public HCatSourceTarget(String table) {
    this(DEFAULT_DATABASE_NAME, table);
  }

  /**
   * Creates a new instance to read from the specified {@code database} and
   * {@code table}
   *
   * @param database
   *          the database to read from
   * @param table
   *          the table to read from
   * @throw IllegalArgumentException if table is null or empty
   */
  public HCatSourceTarget(String database, String table) {
    this(database, table, null);
  }

  /**
   * Creates a new instance to read from the specified {@code database} and
   * {@code table}, restricting partitions by the specified {@code filter}. If
   * the database isn't specified it will default to the
   * {@link org.apache.hadoop.hive.metastore.MetaStoreUtils#DEFAULT_DATABASE_NAME
   * default} database.
   *
   * @param database
   *          the database to read from
   * @param table
   *          the table to read from
   * @param filter
   *          the filter to apply to find partitions
   * @throw IllegalArgumentException if table is null or empty
   */
  public HCatSourceTarget(@Nullable String database, String table, String filter) {
    super(database, table);
    this.database = Strings.isNullOrEmpty(database) ? DEFAULT_DATABASE_NAME : database;
    Preconditions.checkArgument(!StringUtils.isEmpty(table), "table cannot be null or empty");
    this.table = table;
    this.filter = filter;
  }

  @Override
  public SourceTarget<HCatRecord> conf(String key, String value) {
    return null;
  }

  @Override
  public Source<HCatRecord> inputConf(String key, String value) {
    bundle.set(key, value);
    return this;
  }

  @Override
  public PType<HCatRecord> getType() {
    return PTYPE;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter() {
    return PTYPE.getConverter();
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    Configuration jobConf = job.getConfiguration();

    if (hcatConf == null) {
      hcatConf = configureHCatFormat(jobConf, bundle, database, table, filter);
    }

    if (inputId == -1) {
      job.setMapperClass(CrunchMapper.class);
      job.setInputFormatClass(bundle.getFormatClass());
      bundle.configure(jobConf);
    } else {
      Path dummy = new Path("/hcat/" + database + "/" + table);
      CrunchInputs.addInputPath(job, dummy, bundle, inputId);
    }
  }

  static Configuration configureHCatFormat(Configuration conf, FormatBundle<HCatInputFormat> bundle, String database,
      String table, String filter) {
    // It is tricky to get the HCatInputFormat configured correctly.
    //
    // The first parameter of setInput() is for both input and output.
    // It reads Hive MetaStore's JDBC URL or HCatalog server's Thrift address,
    // and saves the schema into the configuration for runtime needs
    // (e.g. data location).
    //
    // Our solution is to create another configuration object, and
    // compares with the original one to see what has been added.
    Configuration newConf = new Configuration(conf);
    try {
      HCatInputFormat.setInput(newConf, database, table, filter);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }

    for (Map.Entry<String, String> e : newConf) {
      String key = e.getKey();
      String value = e.getValue();
      if (!Objects.equal(value, conf.get(key))) {
        bundle.set(key, value);
      }
    }

    return newConf;
  }

  @Override
  public long getSize(Configuration conf) {

    // this is tricky. we want to derive the size by the partitions being
    // retrieved. these aren't known until after the HCatInputFormat has
    // been initialized (see #configureHCatFormat). preferably, the input
    // format shouldn't be configured twice to cut down on the number of calls
    // to hive. getSize can be called before configureSource is called when the
    // collection is being materialized or a groupby has been performed. so, the
    // InputJobInfo, which has the partitions, won't be present when this
    // happens. so, configure here or in configureSource just once.
    if (hcatConf == null) {
      hcatConf = configureHCatFormat(conf, bundle, database, table, filter);
    }
    try {
      InputJobInfo inputJobInfo = (InputJobInfo) HCatUtil.deserialize(hcatConf.get(HCatConstants.HCAT_KEY_JOB_INFO));
      List<PartInfo> partitions = inputJobInfo.getPartitions();

      if (partitions.size() > 0) {
        LOGGER.debug("Found [{}] partitions to read", partitions.size());
        long size = 0;
        for (final PartInfo partition : partitions) {
          String totalSize = partition.getInputStorageHandlerProperties().getProperty(StatsSetupConst.TOTAL_SIZE);

          if (StringUtils.isEmpty(totalSize)) {
            long pathSize = SourceTargetHelper.getPathSize(conf, new Path(partition.getLocation()));
            if (pathSize == -1) {
              LOGGER.info("Unable to locate directory [{}]; skipping", partition.getLocation());
              // could be an hbase table, in which there won't be a size
              // estimate if this is a valid native table partition, but no
              // data, materialize won't find anything
            } else if (pathSize == 0) {
              size += DEFAULT_ESTIMATE;
            } else {
              size += pathSize;
            }
          } else {
            size += Long.parseLong(totalSize);
          }
        }
        return size;
      } else {
        Table hiveTable = getHiveTable(conf);
        LOGGER.debug("Attempting to get table size from table properties for table [{}]", table);

        // managed table will have the size on it, but should be caught as a
        // partition.size == 1 if the table isn't partitioned
        String totalSize = hiveTable.getParameters().get(StatsSetupConst.TOTAL_SIZE);
        if (!StringUtils.isEmpty(totalSize))
          return Long.parseLong(totalSize);

        // not likely to be hit. the totalSize should have been available on the
        // partitions returned (for unpartitioned tables one partition will be
        // returned, referring to the entire table), or on the table metadata
        // (only there for managed tables). if neither existed, then check
        // against the data location as backup. note: external tables can be
        // somewhere other than the root location as defined by the table,
        // as partitions can exist elsewhere. ideally this scenario is caught
        // by the if statement with partitions > 0
        LOGGER.debug("Unable to find size on table properties [{}], attempting to get it from table data location [{}]",
            hiveTable.getTableName(), hiveTable.getDataLocation());
        return SourceTargetHelper.getPathSize(conf, hiveTable.getDataLocation());
      }
    } catch (IOException | TException e) {
      LOGGER.info("Unable to determine an estimate for requested table [{}], using default", table, e);
      return DEFAULT_ESTIMATE;
    }
  }

  /**
   * Extracts the {@link HCatSchema} from the specified {@code conf}.
   *
   * @param conf
   *          the conf containing the table schema
   * @return the HCatSchema
   *
   * @throws TException
   *           if there was an issue communicating with the metastore
   * @throws IOException
   *           if there was an issue connecting to the metastore
   */
  public HCatSchema getTableSchema(Configuration conf) throws TException, IOException {
    Table hiveTable = getHiveTable(conf);
    return HCatUtil.extractSchema(hiveTable);
  }

  @Override
  public long getLastModifiedAt(Configuration conf) {
    LOGGER.warn("Unable to determine the last modified time for db [{}] and table [{}]", database, table);
    return -1;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    HCatSourceTarget that = (HCatSourceTarget) o;
    return Objects.equal(this.database, that.database) && Objects.equal(this.table, that.table)
        && Objects.equal(this.filter, that.filter);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(table, database, filter);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("database", database).append("table", table).append("filter", filter)
        .toString();
  }

  private Table getHiveTable(Configuration conf) throws IOException, TException {
    if (hiveTableCached != null) {
      return hiveTableCached;
    }

    IMetaStoreClient hiveMetastoreClient = HCatUtil.getHiveMetastoreClient(new HiveConf(conf, HCatSourceTarget.class));
    hiveTableCached = HCatUtil.getTable(hiveMetastoreClient, database, table);
    return hiveTableCached;
  }

  @Override
  public Iterable<HCatRecord> read(Configuration conf) throws IOException {
    if (hcatConf == null) {
      hcatConf = configureHCatFormat(conf, bundle, database, table, filter);
    }

    return new HCatRecordDataIterable(bundle, hcatConf);
  }

  @Override
  public ReadableData<HCatRecord> asReadable() {
    return new HCatRecordDataReadable(bundle, database, table, filter);
  }
}
