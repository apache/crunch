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

import org.apache.crunch.Target;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;

/**
 * Static factory methods for creating HBase {@link Target} types.
 */
public class ToHBase {

  public static Target table(String table) {
    return table(TableName.valueOf(table));
  }

  public static Target table(TableName table) {
    return new HBaseTarget(table);
  }

  public static Target hfile(String path) {
    return new HFileTarget(path);
  }

  public static Target hfile(Path path) {
    return new HFileTarget(path);
  }
}
