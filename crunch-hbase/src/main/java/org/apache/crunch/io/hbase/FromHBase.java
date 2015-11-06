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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Static factory methods for creating HBase {@link Source} types.
 */
public class FromHBase {

  public static TableSource<ImmutableBytesWritable, Result> table(String table) {
    return table(table, new Scan());
  }

  public static TableSource<ImmutableBytesWritable, Result> table(String table, Scan scan) {
    return table(TableName.valueOf(table), scan);
  }

  public static TableSource<ImmutableBytesWritable, Result> table(String table, List<Scan> scans) {
    return table(TableName.valueOf(table), scans);
  }

  public static TableSource<ImmutableBytesWritable, Result> table(TableName table) {
    return table(table, new Scan());
  }

  public static TableSource<ImmutableBytesWritable, Result> table(TableName table, Scan scan) {
    return table(table, ImmutableList.of(scan));
  }

  public static TableSource<ImmutableBytesWritable, Result> table(TableName table, List<Scan> scans) {
    if (scans.isEmpty()) {
      throw new IllegalArgumentException("Must supply at least one scan");
    }
    return new HBaseSourceTarget(table, scans.toArray(new Scan[scans.size()]));
  }

  public static Source<KeyValue> hfile(String path) {
    return hfile(new Path(path));
  }

  public static Source<KeyValue> hfile(Path path) {
    return new HFileSource(path);
  }
}
