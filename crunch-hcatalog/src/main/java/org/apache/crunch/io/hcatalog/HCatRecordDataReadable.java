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
import com.google.common.collect.ImmutableSet;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.FormatBundle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class HCatRecordDataReadable implements ReadableData<HCatRecord> {

  private final FormatBundle<HCatInputFormat> bundle;
  private final String database;
  private final String table;
  private final String filter;

  public HCatRecordDataReadable(FormatBundle<HCatInputFormat> bundle, String database, String table, String filter) {
    this.bundle = bundle;
    this.database = database;
    this.table = table;
    this.filter = filter;
  }

  @Override
  public Set<SourceTarget<?>> getSourceTargets() {
    return ImmutableSet.of();
  }

  @Override
  public void configure(Configuration conf) {
    // need to configure the input format, so the JobInputInfo is populated with
    // the partitions to be processed. the partitions are needed to derive the
    // input splits and to get a size estimate for the HCatSource.
    HCatSourceTarget.configureHCatFormat(conf, bundle, database, table, filter);
  }

  @Override
  public Iterable<HCatRecord> read(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
    return new HCatRecordDataIterable(bundle, context.getConfiguration());
  }
}
