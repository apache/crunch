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
package org.apache.crunch.impl.mr.run;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CrunchCombineFileInputFormat<K, V> extends CombineFileInputFormat<K, V> {

  public CrunchCombineFileInputFormat(JobContext jobContext) {
    if (getMaxSplitSize(jobContext) == Long.MAX_VALUE) {
      Configuration conf = jobContext.getConfiguration();
      if (conf.get(RuntimeParameters.COMBINE_FILE_BLOCK_SIZE) != null) {
        setMaxSplitSize(conf.getLong(RuntimeParameters.COMBINE_FILE_BLOCK_SIZE, 0));
      } else {
        setMaxSplitSize(jobContext.getConfiguration().getLongBytes("dfs.blocksize", 134217728L));
      }
    }
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    throw new UnsupportedOperationException("CrunchCombineFileInputFormat.createRecordReader should never be called");
  }
}
