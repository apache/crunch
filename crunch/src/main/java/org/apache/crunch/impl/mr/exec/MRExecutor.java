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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchJobControl;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.materialize.MaterializableIterable;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

/**
 *
 *
 */
public class MRExecutor {

  private static final Log LOG = LogFactory.getLog(MRExecutor.class);

  private final CrunchJobControl control;
  private final Map<PCollectionImpl<?>, Set<Target>> outputTargets;
  private final Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize;
  
  private String planDotFile;
  
  public MRExecutor(Class<?> jarClass, Map<PCollectionImpl<?>, Set<Target>> outputTargets,
      Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize) {
    this.control = new CrunchJobControl(jarClass.toString());
    this.outputTargets = outputTargets;
    this.toMaterialize = toMaterialize;
  }

  public void addJob(CrunchJob job) {
    this.control.addJob(job);
  }

  public void setPlanDotFile(String planDotFile) {
    this.planDotFile = planDotFile;
  }
  
  public PipelineExecution execute() {
    FutureImpl fi = new FutureImpl();
    fi.init();
    return fi;
  }
  
  private class FutureImpl extends AbstractFuture<PipelineResult> implements PipelineExecution {
    @Override
    public String getPlanDotFile() {
      return planDotFile;
    }
    
    public void init() {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            Thread controlThread = new Thread(control);
            controlThread.start();
            while (!control.allFinished()) {
              Thread.sleep(1000);
            }
            control.stop();
          } catch (InterruptedException e) {
            setException(e);
            return;
          }
          
          List<CrunchControlledJob> failures = control.getFailedJobList();
          if (!failures.isEmpty()) {
            System.err.println(failures.size() + " job failure(s) occurred:");
            for (CrunchControlledJob job : failures) {
              System.err.println(job.getJobName() + "(" + job.getJobID() + "): " + job.getMessage());
            }
          }
          List<PipelineResult.StageResult> stages = Lists.newArrayList();
          for (CrunchControlledJob job : control.getSuccessfulJobList()) {
            try {
              stages.add(new PipelineResult.StageResult(job.getJobName(), job.getJob().getCounters()));
            } catch (Exception e) {
              LOG.error("Exception thrown fetching job counters for stage: " + job.getJobName(), e);
            }
          }
          
          for (PCollectionImpl<?> c : outputTargets.keySet()) {
            if (toMaterialize.containsKey(c)) {
              MaterializableIterable iter = toMaterialize.get(c);
              if (iter.isSourceTarget()) {
                iter.materialize();
                c.materializeAt((SourceTarget) iter.getSource());
              }
            } else {
              boolean materialized = false;
              for (Target t : outputTargets.get(c)) {
                if (!materialized) {
                  if (t instanceof SourceTarget) {
                    c.materializeAt((SourceTarget) t);
                    materialized = true;
                  } else {
                    SourceTarget st = t.asSourceTarget(c.getPType());
                    if (st != null) {
                      c.materializeAt(st);
                      materialized = true;
                    }
                  }
                }
              }
            }
          }

          set(new PipelineResult(stages));
        }
      };
      t.start();
    }
    
    @Override
    public void interruptTask() {
      if (!control.allFinished()) {
        control.stop();
      }
      for (CrunchControlledJob job : control.getRunningJobList()) {
        if (!job.isCompleted()) {
          try {
            job.killJob();
          } catch (Exception e) {
            LOG.error("Exception killing job: " + job.getJobName(), e);
          }
        }
      }
    }
  }
}
