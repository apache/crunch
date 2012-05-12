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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

import com.cloudera.crunch.io.impl.InputBundle;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CrunchInputs {

  private static final char RECORD_SEP = ',';
  private static final char FIELD_SEP = ';';
  private static final Joiner JOINER = Joiner.on(FIELD_SEP);
  private static final Splitter SPLITTER = Splitter.on(FIELD_SEP);
  
  public static void addInputPath(Job job, Path path,
      InputBundle inputBundle, int nodeIndex) {
    Configuration conf = job.getConfiguration();
    String inputs = JOINER.join(inputBundle.serialize(), String.valueOf(nodeIndex), path.toString());
    String existing = conf.get(RuntimeParameters.MULTI_INPUTS);
    conf.set(RuntimeParameters.MULTI_INPUTS, existing == null ? inputs : existing + RECORD_SEP
        + inputs);
  }

  public static Map<InputBundle, Map<Integer, List<Path>>> getFormatNodeMap(
      JobContext job) {
    Map<InputBundle, Map<Integer, List<Path>>> formatNodeMap = Maps.newHashMap();
    Configuration conf = job.getConfiguration();
    for (String input : Splitter.on(RECORD_SEP).split(conf.get(RuntimeParameters.MULTI_INPUTS))) {
      List<String> fields = Lists.newArrayList(SPLITTER.split(input));
      InputBundle inputBundle = InputBundle.fromSerialized(fields.get(0));
      if (!formatNodeMap.containsKey(inputBundle)) {
        formatNodeMap.put(inputBundle, Maps.<Integer, List<Path>> newHashMap());
      }
      Integer nodeIndex = Integer.valueOf(fields.get(1));
      if (!formatNodeMap.get(inputBundle).containsKey(nodeIndex)) {
        formatNodeMap.get(inputBundle).put(nodeIndex,
            Lists.<Path> newLinkedList());
      }
      formatNodeMap.get(inputBundle).get(nodeIndex)
          .add(new Path(fields.get(2)));
    }
    return formatNodeMap;
  }

}
