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

package com.cloudera.crunch.impl.mr.run;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.crunch.io.impl.InputBundle;
import com.google.common.collect.Lists;

public class CrunchInputFormat<K, V> extends InputFormat<K, V> {

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException,
      InterruptedException {
    List<InputSplit> splits = Lists.newArrayList();
    Configuration conf = job.getConfiguration();
    Map<InputBundle, Map<Integer, List<Path>>> formatNodeMap = CrunchInputs.getFormatNodeMap(job);

    // First, build a map of InputFormats to Paths
    for (Map.Entry<InputBundle, Map<Integer, List<Path>>> entry : formatNodeMap.entrySet()) {
      InputBundle inputBundle = entry.getKey();
      Job jobCopy = new Job(conf);
      InputFormat format = (InputFormat) ReflectionUtils.newInstance(
          inputBundle.getInputFormatClass(), jobCopy.getConfiguration());
      for (Map.Entry<Integer, List<Path>> nodeEntry : entry.getValue()
          .entrySet()) {
        Integer nodeIndex = nodeEntry.getKey();
        List<Path> paths = nodeEntry.getValue();
        FileInputFormat.setInputPaths(jobCopy, paths.toArray(new Path[paths.size()]));

        // Get splits for each input path and tag with InputFormat
        // and Mapper types by wrapping in a TaggedInputSplit.
        List<InputSplit> pathSplits = format.getSplits(jobCopy);
        for (InputSplit pathSplit : pathSplits) {
          splits.add(new CrunchInputSplit(pathSplit, inputBundle.getInputFormatClass(),
              inputBundle.getExtraConfiguration(), nodeIndex, jobCopy.getConfiguration()));
        }
      }
    }
    return splits;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new CrunchRecordReader(inputSplit, context);
  }

}
