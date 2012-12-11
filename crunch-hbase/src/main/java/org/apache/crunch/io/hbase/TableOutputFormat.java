/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package org.apache.crunch.io.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;

class TableOutputFormat<K> extends OutputFormat<K, Writable> {

  private final Log LOG = LogFactory.getLog(TableOutputFormat.class);

  /** Job parameter that specifies the output table. */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";

  /**
   * Optional job parameter to specify a peer cluster.
   * Used specifying remote cluster when copying between hbase clusters (the
   * source is picked up from <code>hbase-site.xml</code>).
   * @see TableMapReduceUtil#initTableReducerJob(String, Class, org.apache.hadoop.mapreduce.Job, Class, String, String, String)
   */
  public static final String QUORUM_ADDRESS = "hbase.mapred.output.quorum";

  /** Optional specification of the rs class name of the peer cluster */
  public static final String
      REGION_SERVER_CLASS = "hbase.mapred.output.rs.class";
  /** Optional specification of the rs impl name of the peer cluster */
  public static final String
      REGION_SERVER_IMPL = "hbase.mapred.output.rs.impl";
  
  
  private final Map<String, HTable> tables = Maps.newHashMap();
  
  private static class TableRecordWriter<K> extends RecordWriter<K, Writable> {

    private HTable table;

    public TableRecordWriter(HTable table) {
      this.table = table;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
      table.close();
    }

    @Override
    public void write(K key, Writable value)
    throws IOException {
      if (value instanceof Put) this.table.put(new Put((Put)value));
      else if (value instanceof Delete) this.table.delete(new Delete((Delete)value));
      else throw new IOException("Pass a Delete or a Put");
    }
  }
  
  @Override
  public void checkOutputSpecs(JobContext jc) throws IOException, InterruptedException {
    // No-op for now
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext ctxt) throws IOException,
      InterruptedException {
    return new TableOutputCommitter();
  }

  @Override
  public RecordWriter<K, Writable> getRecordWriter(TaskAttemptContext ctxt) throws IOException,
      InterruptedException {
    Configuration conf = ctxt.getConfiguration();
    String tableName = conf.get(OUTPUT_TABLE);
    if(tableName == null || tableName.length() <= 0) {
      throw new IllegalArgumentException("Must specify table name");
    }
    HTable table = tables.get(tableName);
    if (table == null) {
      conf = HBaseConfiguration.create(conf);
      String address = conf.get(QUORUM_ADDRESS);
      String serverClass = conf.get(REGION_SERVER_CLASS);
      String serverImpl = conf.get(REGION_SERVER_IMPL);
      try {
        if (address != null) {
          ZKUtil.applyClusterKeyToConf(conf, address);
        }
        if (serverClass != null) {
          conf.set(HConstants.REGION_SERVER_CLASS, serverClass);
          conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
        }
        table = new HTable(conf, tableName);
        table.setAutoFlush(false);
        tables.put(tableName, table);
      } catch (IOException e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
    }
    return new TableRecordWriter<K>(table);
  }

}
