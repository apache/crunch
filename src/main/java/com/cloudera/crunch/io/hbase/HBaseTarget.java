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

import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;
import com.cloudera.crunch.io.MapReduceTarget;
import com.cloudera.crunch.io.OutputHandler;
import com.cloudera.crunch.types.PType;

public class HBaseTarget implements MapReduceTarget {

  protected String table;
  
  public HBaseTarget(String table) {
    this.table = table;
  }
  
  @Override
  public boolean equals(Object other) {
    if(this == other)
      return true;
    if(other == null)
      return false;
    if(!other.getClass().equals(getClass()))
      return false;
    HBaseTarget o = (HBaseTarget)other;
    return table.equals(o.table);
  }
  
  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(table).toHashCode();
  }
  
  @Override
  public String toString() {
    return "HBaseTable(" + table + ")";
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if(Put.class.equals(ptype.getTypeClass())) {
      handler.configure(this, ptype);
      return true;      
    }
    return false;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.addHbaseResources(conf);
    job.setOutputFormatClass(TableOutputFormat.class);
    conf.set(TableOutputFormat.OUTPUT_TABLE, table);
    try {
      TableMapReduceUtil.addDependencyJars(job);      
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    return null;
  }
}
