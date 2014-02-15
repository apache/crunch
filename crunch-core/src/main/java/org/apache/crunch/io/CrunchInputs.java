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
package org.apache.crunch.io;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Helper functions for configuring multiple {@code InputFormat} instances within a single
 * Crunch MapReduce job.
 */
public class CrunchInputs {
  public static final String CRUNCH_INPUTS = "crunch.inputs.dir";

  private static final char RECORD_SEP = ',';
  private static final char FIELD_SEP = ';';
  private static final char PATH_SEP = '|';
  private static final Joiner JOINER = Joiner.on(FIELD_SEP);
  private static final Splitter SPLITTER = Splitter.on(FIELD_SEP);

  public static void addInputPath(Job job, Path path, FormatBundle inputBundle, int nodeIndex) {
    addInputPaths(job, Collections.singleton(path), inputBundle, nodeIndex);
  }

  public static void addInputPaths(Job job, Collection<Path> paths, FormatBundle inputBundle, int nodeIndex) {
    Configuration conf = job.getConfiguration();
    List<String> encodedPathStrs = Lists.newArrayListWithExpectedSize(paths.size());
    for (Path path : paths) {
      String pathStr = encodePath(path);
      Preconditions.checkArgument(pathStr.indexOf(RECORD_SEP) == -1 && pathStr.indexOf(FIELD_SEP) == -1 && pathStr.indexOf(PATH_SEP) == -1);
      encodedPathStrs.add(pathStr);
    }
    String inputs = JOINER.join(inputBundle.serialize(), String.valueOf(nodeIndex), Joiner.on(PATH_SEP).join(encodedPathStrs));
    String existing = conf.get(CRUNCH_INPUTS);
    conf.set(CRUNCH_INPUTS, existing == null ? inputs : existing + RECORD_SEP + inputs);
  }

  public static Map<FormatBundle, Map<Integer, List<Path>>> getFormatNodeMap(JobContext job) {
    Map<FormatBundle, Map<Integer, List<Path>>> formatNodeMap = Maps.newHashMap();
    Configuration conf = job.getConfiguration();
    String crunchInputs = conf.get(CRUNCH_INPUTS);
    if (crunchInputs == null || crunchInputs.isEmpty()) {
      return ImmutableMap.of();
    }
    for (String input : Splitter.on(RECORD_SEP).split(crunchInputs)) {
      List<String> fields = Lists.newArrayList(SPLITTER.split(input));
      FormatBundle<InputFormat> inputBundle = FormatBundle.fromSerialized(fields.get(0), job.getConfiguration());
      if (!formatNodeMap.containsKey(inputBundle)) {
        formatNodeMap.put(inputBundle, Maps.<Integer, List<Path>>newHashMap());
      }
      Integer nodeIndex = Integer.valueOf(fields.get(1));
      if (!formatNodeMap.get(inputBundle).containsKey(nodeIndex)) {
        formatNodeMap.get(inputBundle).put(nodeIndex, Lists.<Path>newLinkedList());
      }
      List<Path> formatNodePaths = formatNodeMap.get(inputBundle).get(nodeIndex);
      String encodedPaths = fields.get(2);
      for (String encodedPath : Splitter.on(PATH_SEP).split(encodedPaths)) {
        formatNodePaths.add(decodePath(encodedPath));
      }
    }
    return formatNodeMap;
  }

  private static String encodePath(Path path) {
    try {
      return URLEncoder.encode(path.toString(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static Path decodePath(String encodedPath) {
    try {
      return new Path(URLDecoder.decode(encodedPath, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
