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
package org.apache.crunch.impl.mr.exec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob;
import org.apache.crunch.impl.mr.MRJob;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.PathTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public final class CrunchJobHooks {

  private CrunchJobHooks() {}

  public static final class CompositeHook implements CrunchControlledJob.Hook {

    private List<CrunchControlledJob.Hook> hooks;

    public CompositeHook(List<CrunchControlledJob.Hook> hooks) {
      this.hooks = hooks;
    }

    @Override
    public void run(MRJob job) throws IOException {
      for (CrunchControlledJob.Hook hook : hooks) {
        hook.run(job);
      }
    }
  }

  /** Creates missing input directories before job is submitted. */
  public static final class PrepareHook implements CrunchControlledJob.Hook {
    @Override
    public void run(MRJob job) throws IOException {
      Configuration conf = job.getJob().getConfiguration();
      if (conf.getBoolean(RuntimeParameters.CREATE_DIR, false)) {
        Path[] inputPaths = FileInputFormat.getInputPaths(job.getJob());
        for (Path inputPath : inputPaths) {
          FileSystem fs = inputPath.getFileSystem(conf);
          if (!fs.exists(inputPath)) {
            try {
              fs.mkdirs(inputPath);
            } catch (IOException e) {
            }
          }
        }
      }
    }
  }

  /** Moving output files produced by the MapReduce job to specified directories. */
  public static final class CompletionHook implements CrunchControlledJob.Hook {
    private final Path workingPath;
    private final Map<Integer, PathTarget> multiPaths;

    public CompletionHook(Path workingPath, Map<Integer, PathTarget> multiPaths) {
      this.workingPath = workingPath;
      this.multiPaths = multiPaths;
    }

    @Override
    public void run(MRJob job) throws IOException {
      handleMultiPaths(job.getJob());
    }

    private synchronized void handleMultiPaths(Job job) throws IOException {
      try {
        if (job.isSuccessful()) {
          if (!multiPaths.isEmpty()) {
            for (Map.Entry<Integer, PathTarget> entry : multiPaths.entrySet()) {
              entry.getValue().handleOutputs(job.getConfiguration(), workingPath, entry.getKey());
            }
          }
        }
      } catch(Exception ie) {
        throw new IOException(ie);
      }
    }
  }
}
