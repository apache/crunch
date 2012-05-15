/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.impl.mr.exec;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.crunch.impl.mr.plan.MSCROutputHandler;
import com.cloudera.crunch.impl.mr.plan.PlanningParameters;
import com.google.common.collect.Lists;

public class CrunchJob extends CrunchControlledJob {

  private final Log log = LogFactory.getLog(CrunchJob.class);
  
  private final Path workingPath;
  private final List<Path> multiPaths;
  private final boolean mapOnlyJob;
  
  public CrunchJob(Job job, Path workingPath, MSCROutputHandler handler) throws IOException {
    super(job, Lists.<CrunchControlledJob>newArrayList());
    this.workingPath = workingPath;
    this.multiPaths = handler.getMultiPaths();
    this.mapOnlyJob = handler.isMapOnlyJob();
  }  
  
  private synchronized void handleMultiPaths() throws IOException {
    if (!multiPaths.isEmpty()) {
      // Need to handle moving the data from the output directory of the
      // job to the output locations specified in the paths.
      FileSystem fs = FileSystem.get(job.getConfiguration());
      for (int i = 0; i < multiPaths.size(); i++) {
        Path src = new Path(workingPath,
            PlanningParameters.MULTI_OUTPUT_PREFIX + i + "-*");
        Path[] srcs = FileUtil.stat2Paths(fs.globStatus(src), src);
        Path dst = multiPaths.get(i);
        if (!fs.exists(dst)) {
          fs.mkdirs(dst);
        }
        int minPartIndex = getMinPartIndex(dst, fs);
        for (Path s : srcs) {
          fs.rename(s, getDestFile(s, dst, minPartIndex++));
        }
      }
    }
  }
  
  private Path getDestFile(Path src, Path dir, int index) {
    String form = "part-%s-%05d";
    if (src.getName().endsWith(org.apache.avro.mapred.AvroOutputFormat.EXT)) {
      form = form + org.apache.avro.mapred.AvroOutputFormat.EXT;
    }
    return new Path(dir, String.format(form, mapOnlyJob ? "m" : "r", index));
  }
  
  private int getMinPartIndex(Path path, FileSystem fs) throws IOException {
    // Quick and dirty way to ensure unique naming in the directory
    return fs.listStatus(path).length;
  }
  
  @Override
  protected void checkRunningState() throws IOException, InterruptedException {
    try {
      if (job.isComplete()) {
        if (job.isSuccessful()) {
          handleMultiPaths();
          this.state = State.SUCCESS;
        } else {
          this.state = State.FAILED;
          this.message = "Job failed!";
        }
      }
    } catch (IOException ioe) {
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
      try {
        if (job != null) {
          job.killJob();
        }
      } catch (IOException e) {
      }
    }
  }

  @Override
  protected synchronized void submit() {
    super.submit();
    if (this.state == State.RUNNING) {
      log.info("Running job \"" + getJobName() + "\"");
      log.info("Job status available at: " + job.getTrackingURL());
    } else {
      log.info("Error occurred starting job \"" + getJobName() + "\":");
      log.info(getMessage());
    }
  }
}
