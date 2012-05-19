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
package com.cloudera.crunch.io.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.impl.mr.run.CrunchMapper;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.writable.Writables;

public class HBaseSourceTarget extends HBaseTarget implements SourceTarget<Pair<ImmutableBytesWritable, Result>>,
    TableSource<ImmutableBytesWritable, Result> {

  private static final PTableType<ImmutableBytesWritable, Result> PTYPE = Writables.tableOf(
      Writables.writables(ImmutableBytesWritable.class), Writables.writables(Result.class));
  
  protected Scan scan;
  
  public HBaseSourceTarget(String table, Scan scan) {
    super(table);
    this.scan = scan;
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
    return table.equals(o.table) && scan.equals(o.scan);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(table).append(scan).toHashCode();
  }
  
  @Override
  public String toString() {
    return "HBaseTable(" + table + ")";
  }
  
  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    Configuration conf = job.getConfiguration();
    job.setInputFormatClass(TableInputFormat.class);
    job.setMapperClass(CrunchMapper.class);
    HBaseConfiguration.addHbaseResources(conf);
    conf.set(TableInputFormat.INPUT_TABLE, table);
    conf.set(TableInputFormat.SCAN, convertScanToString(scan));
    TableMapReduceUtil.addDependencyJars(job);
  }
  
  static String convertScanToString(Scan scan) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(out);
    scan.write(dos);
    return Base64.encodeBytes(out.toByteArray());
  }

  @Override
  public long getSize(Configuration conf) {
    // TODO something smarter here.
    return 1000L * 1000L * 1000L;
  }
}
