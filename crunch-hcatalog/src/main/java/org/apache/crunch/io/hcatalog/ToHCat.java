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

import org.apache.crunch.Target;

import java.util.Map;

/**
 * Factory static helper methods for writing data to HCatalog
 *
 * <pre>
 * {@code
 *  Pipeline pipeline = new MRPipeline(this.getClass());
 *
 *  PCollection<HCatRecord> hcatRecords = pipeline.read(FromHCat.table("this-table");
 *
 *  pipeline.write(hcatRecords, ToHCat.table("that-table"));
 * }
 * </pre>
 */
public class ToHCat {

  /**
   * Constructs a new instance to write to the provided hive {@code table} name.
   * Writes to the "default" database.
   *
   * Note: if the destination table is partitioned, this constructor should not
   * be used. It will only be usable by unpartitioned tables
   *
   * @param tableName
   *          the hive table to write to
   */
  public static Target table(String tableName) {
    return new HCatTarget(tableName);
  }

  /**
   * Constructs a new instance to write to the provided hive {@code tableName},
   * using the provided {@code database}. If null, uses "default" database.
   *
   * Note: if the destination table is partitioned, this constructor should not
   * be used. It will only be usable by unpartitioned tables
   *
   * @param database
   *          the hive database to use for table namespacing
   * @param tableName
   *          the hive table to write to
   */
  public static Target table(String database, String tableName) {
    return new HCatTarget(database, tableName);
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
   * @param tableName
   *          the hive table to write to
   * @param partition
   *          the partition within the table it should be written
   */
  public static Target table(String tableName, Map<String, String> partition) {
    return new HCatTarget(tableName, partition);
  }

  /**
   * Constructs a new instance to write to the provided {@code database},
   * {@code tableName}, and to the specified {@code partition}. If
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
   * @param tableName
   *          the hive table to write to
   * @param partition
   *          the partition within the table it should be written
   */
  public static Target table(String database, String tableName, Map<String, String> partition) {
    return new HCatTarget(database, tableName, partition);
  }
}
