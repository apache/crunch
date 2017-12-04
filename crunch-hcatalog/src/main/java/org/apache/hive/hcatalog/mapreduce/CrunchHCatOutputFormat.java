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
package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;

/**
 * Thin extension to supply {@link OutputFormat}'s that carrying out crunch
 * specific logic
 */
public class CrunchHCatOutputFormat extends HCatOutputFormat {

  @Override
  protected OutputFormat<WritableComparable<?>, HCatRecord> getOutputFormat(JobContext context) throws IOException {
    OutputJobInfo jobInfo = getJobInfo(context.getConfiguration());
    HiveStorageHandler storageHandler = HCatUtil.getStorageHandler(context.getConfiguration(),
        jobInfo.getTableInfo().getStorerInfo());
    // Always configure storage handler with jobproperties/jobconf before
    // calling any methods on it
    configureOutputStorageHandler(context);
    if (storageHandler instanceof FosterStorageHandler) {
      return new CrunchFileOutputFormatContainer(
          ReflectionUtils.newInstance(storageHandler.getOutputFormatClass(), context.getConfiguration()));
    } else {
      return new CrunchDefaultOutputFormatContainer(
          ReflectionUtils.newInstance(storageHandler.getOutputFormatClass(), context.getConfiguration()));
    }
  }
}
