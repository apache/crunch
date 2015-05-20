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
package org.apache.crunch.impl.spark;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.SparkCounter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkFiles;
import org.apache.spark.broadcast.Broadcast;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class SparkRuntimeContext implements Serializable {

  private String jobName;
  private Broadcast<byte[]> broadConf;
  private final Accumulator<Map<String, Map<String, Long>>> counters;
  private transient Configuration conf;
  private transient TaskInputOutputContext context;
  private transient Integer lastTID;

  public SparkRuntimeContext(
      String jobName,
      Accumulator<Map<String, Map<String, Long>>> counters,
      Broadcast<byte[]> broadConf) {
    this.jobName = jobName;
    this.counters = counters;
    this.broadConf = broadConf;
  }

  public void setConf(Broadcast<byte[]> broadConf) {
    this.broadConf = broadConf;
    this.conf = null;
  }

  public void initialize(DoFn<?, ?> fn, Integer tid) {
    if (context == null || !Objects.equal(lastTID, tid)) {
      TaskAttemptID attemptID;
      if (tid != null) {
        TaskID taskId = new TaskID(new JobID(jobName, 0), false, tid);
        attemptID = new TaskAttemptID(taskId, 0);
        lastTID = tid;
      } else {
        attemptID = new TaskAttemptID();
        lastTID = null;
      }
      configureLocalFiles();
      context = new MapContextImpl(getConfiguration(), attemptID, null, null, null, new SparkReporter(counters), null);
    }
    fn.setContext(context);
    fn.initialize();
  }

  private void configureLocalFiles() {
    try {
      URI[] uris = DistributedCache.getCacheFiles(getConfiguration());
      if (uris != null) {
        List<String> allFiles = Lists.newArrayList();
        for (URI uri : uris) {
          File f = new File(uri.getPath());
          allFiles.add(SparkFiles.get(f.getName()));
        }
        String sparkFiles = Joiner.on(',').join(allFiles);
        // Hacking this for Hadoop1 and Hadoop2
        getConfiguration().set("mapreduce.job.cache.local.files", sparkFiles);
        getConfiguration().set("mapred.cache.localFiles", sparkFiles);
      }
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  public Configuration getConfiguration() {
    if (conf == null) {
      conf = new Configuration();
      try {
        conf.readFields(ByteStreams.newDataInput(broadConf.value()));
      } catch (Exception e) {
        throw new RuntimeException("Error reading broadcast configuration", e);
      }
    }
    return conf;
  }

  private static class SparkReporter extends StatusReporter implements Serializable {

    Accumulator<Map<String, Map<String, Long>>> accum;
    private transient Map<String, Map<String, Counter>> counters;

    public SparkReporter(Accumulator<Map<String, Map<String, Long>>> accum) {
      this.accum = accum;
      this.counters = Maps.newHashMap();
    }

    @Override
    public Counter getCounter(Enum<?> anEnum) {
      return getCounter(anEnum.getDeclaringClass().toString(), anEnum.name());
    }

    @Override
    public Counter getCounter(String group, String name) {
      Map<String, Counter> grp = counters.get(group);
      if (grp == null) {
        grp = Maps.newTreeMap();
        counters.put(group, grp);
      }
      if (!grp.containsKey(name)) {
        grp.put(name, new SparkCounter(group, name, accum));
      }
      return grp.get(name);
    }

    @Override
    public void progress() {
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void setStatus(String s) {

    }
  }

}
