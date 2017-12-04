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

import org.apache.crunch.Source;
import org.apache.hive.hcatalog.data.HCatRecord;

import javax.annotation.Nullable;

/**
 * Static factory methods for creating sources to read from HCatalog.
 *
 * Access examples:
 * <pre>
 * {@code
 *
 *  Pipeline pipeline = new MRPipeline(this.getClass());
 *
 *  PCollection<HCatRecord> hcatRecords = pipeline.read(FromHCat.table("my-table"))
 * }
 * </pre>
 */
public final class FromHCat {

  private FromHCat() {
  }

  /**
   * Creates a {@code Source<HCatRecord>} instance from a hive table in the
   * default database instance "default".
   *
   * @param table
   *          table name
   * @throw IllegalArgumentException if table is null or empty
   */
  public static Source<HCatRecord> table(String table) {
    return new HCatSourceTarget(table);
  }

  /**
   * Creates a {code Source<HCatRecord>} instance from a hive table.
   *
   * @param database
   *          database name
   * @param table
   *          table name
   * @throw IllegalArgumentException if table is null or empty
   */
  public static Source<HCatRecord> table(String database, String table) {
    return new HCatSourceTarget(database, table);
  }

  /**
   * Creates a {code Source<HCatRecord>} instance from a hive table with custom
   * filter criteria. If {@code database} is null, uses the default
   * database instance "default"
   *
   * @param database
   *          database name
   * @param table
   *          table name
   * @param filter
   *          a custom filter criteria, e.g. specify partitions by
   *          {@code 'date= "20140424"'} or {@code 'date < "20140424"'}
   * @throw IllegalArgumentException if table is null or empty
   */
  public static Source<HCatRecord> table(@Nullable String database, String table, String filter) {
    return new HCatSourceTarget(database, table, filter);
  }
}
