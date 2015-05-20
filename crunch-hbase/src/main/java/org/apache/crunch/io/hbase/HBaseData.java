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

import com.google.common.collect.ImmutableSet;
import org.apache.crunch.Pair;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Set;

public class HBaseData implements ReadableData<Pair<ImmutableBytesWritable, Result>> {

  private final String table;
  private final String scansAsString;
  private transient SourceTarget parent;

  public HBaseData(String table, String scansAsString, SourceTarget<?> parent) {
    this.table = table;
    this.scansAsString = scansAsString;
    this.parent = parent;
  }

  @Override
  public Set<SourceTarget<?>> getSourceTargets() {
    if (parent != null) {
      return ImmutableSet.<SourceTarget<?>>of(parent);
    } else {
      return ImmutableSet.of();
    }
  }

  @Override
  public void configure(Configuration conf) {
    // No-op
  }

  @Override
  public Iterable<Pair<ImmutableBytesWritable, Result>> read(
      TaskInputOutputContext<?, ?, ?, ?> ctxt) throws IOException {
    Configuration hconf = HBaseConfiguration.create(ctxt.getConfiguration());
    HTable htable = new HTable(hconf, table);

    String[] scanStrings = StringUtils.getStrings(scansAsString);
    int length = scanStrings == null ? 0 : scanStrings.length;
    Scan[] scans = new Scan[length];
    for(int i = 0; i < length; i++){
      scans[i] = HBaseSourceTarget.convertStringToScan(scanStrings[i]);
    }

    return new HTableIterable(htable, scans);
  }
}
