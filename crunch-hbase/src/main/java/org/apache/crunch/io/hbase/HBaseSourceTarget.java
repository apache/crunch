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

import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.Pair;
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.run.CrunchMapper;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.ObjectArrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSourceTarget extends HBaseTarget implements
    ReadableSourceTarget<Pair<ImmutableBytesWritable, Result>>,
    TableSource<ImmutableBytesWritable, Result> {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceTarget.class);
  
  private static final PTableType<ImmutableBytesWritable, Result> PTYPE = Writables.tableOf(
      Writables.writables(ImmutableBytesWritable.class), HBaseTypes.results());

  protected Scan[] scans;
  protected String scansAsString;
  private FormatBundle<MultiTableInputFormat> inputBundle;
  
  public HBaseSourceTarget(String table, Scan scan) {
    this(table, new Scan[] { scan });
  }
  
  public HBaseSourceTarget(String table, Scan scan, Scan... additionalScans) {
    this(table, ObjectArrays.concat(scan, additionalScans));
  }
  
  public HBaseSourceTarget(String table, Scan[] scans) {
    super(table);
    this.scans = scans;

    try {

      byte[] tableName = Bytes.toBytes(table);
      //Copy scans and enforce that they are for the table specified
      Scan[] tableScans = new Scan[scans.length];
      String[] scanStrings = new String[scans.length];
      for(int i = 0; i < scans.length; i++){
        tableScans[i] =  new Scan(scans[i]);
        //enforce Scan is for same table
        tableScans[i].setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName);
        //Convert the Scan into a String
        scanStrings[i] = convertScanToString(tableScans[i]);
      }
      this.scans = tableScans;
      this.scansAsString = StringUtils.arrayToString(scanStrings);
      this.inputBundle = FormatBundle.forInput(MultiTableInputFormat.class)
          .set(MultiTableInputFormat.SCANS, scansAsString);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Source<Pair<ImmutableBytesWritable, Result>> inputConf(String key, String value) {
    inputBundle.set(key, value);
    return this;
  }

  @Override
  public PType<Pair<ImmutableBytesWritable, Result>> getType() {
    return PTYPE;
  }

  @Override
  public PTableType<ImmutableBytesWritable, Result> getTableType() {
    return PTYPE;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof HBaseSourceTarget)) {
      return false;
    }
    HBaseSourceTarget o = (HBaseSourceTarget) other;
    // XXX scan does not have equals method
    return inputBundle.equals(o.inputBundle);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(table).append(scansAsString).toHashCode();
  }

  @Override
  public String toString() {
    return "HBaseTable(" + table + ")";
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    TableMapReduceUtil.addDependencyJars(job);
    Configuration conf = job.getConfiguration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        ResultSerialization.class.getName());
    if (inputId == -1) {
      job.setMapperClass(CrunchMapper.class);
      job.setInputFormatClass(inputBundle.getFormatClass());
      inputBundle.configure(conf);
    } else {
      Path dummy = new Path("/hbase/" + table);
      CrunchInputs.addInputPath(job, dummy, inputBundle, inputId);
    }
  }

  static String convertScanToString(Scan scan) throws IOException {
    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
    return Base64.encodeBytes(proto.toByteArray());
  }

  public static Scan convertStringToScan(String string) throws IOException {
    ClientProtos.Scan proto = ClientProtos.Scan.parseFrom(Base64.decode(string));
    return ProtobufUtil.toScan(proto);
  }

  @Override
  public long getSize(Configuration conf) {
    // TODO something smarter here.
    return 1000L * 1000L * 1000L;
  }

  @Override
  public long getLastModifiedAt(Configuration configuration) {
    LOG.warn("Cannot determine last modified time for source: {}", toString());
    return -1;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter() {
    return new HBasePairConverter<ImmutableBytesWritable, Result>(
        ImmutableBytesWritable.class,
        Result.class);
  }

  @Override
  public Iterable<Pair<ImmutableBytesWritable, Result>> read(Configuration conf) throws IOException {
    Configuration hconf = HBaseConfiguration.create(conf);
    HTable htable = new HTable(hconf, table);
    return new HTableIterable(htable, scans);
  }

  @Override
  public ReadableData<Pair<ImmutableBytesWritable, Result>> asReadable() {
      return new HBaseData(table, scansAsString, this);
  }

  @Override
  public SourceTarget<Pair<ImmutableBytesWritable, Result>> conf(String key, String value) {
    inputConf(key, value);
    outputConf(key, value);
    return this;
  }

}
